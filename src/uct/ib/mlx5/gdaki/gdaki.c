/**
 * Copyright (c) NVIDIA CORPORATION & AFFILIATES, 2025-2026. ALL RIGHTS RESERVED.
 *
 * See file LICENSE for terms.
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "gdaki.h"

#include <ucs/async/async.h>
#include <ucs/sys/netlink.h>
#include <ucs/sys/sock.h>
#include <ucs/sys/sys.h>
#include <ucs/sys/topo/base/topo.h>
#include <ucs/time/time.h>
#include <ucs/datastruct/array.h>
#include <ucs/datastruct/string_buffer.h>
#include <ucs/algorithm/qsort_r.h>
#include <ucs/type/init_once.h>
#include <ucs/type/serialize.h>
#include <uct/ib/base/ib_verbs.h>
#include <uct/ib/mlx5/rc/rc_mlx5.h>
#include <uct/cuda/base/cuda_util.h>
#include <uct/cuda/base/cuda_ctx.h>
#include <uct/cuda/base/cuda_nvml.h>
#include <uct/cuda/cuda_copy/cuda_copy_md.h>

#include <fcntl.h>
#include <limits.h>
#include <linux/rtnetlink.h>
#include <string.h>

#include "gpunetio/common/doca_gpunetio_verbs_def.h"

#define UCT_GDAKI_MAX_CUDA_DEVICES 64

/* Plane rank of a device which is not on any plane */
#define UCT_GDAKI_PLANE_NONE UINT_MAX

#define UCT_GDAKI_CQ_PAGE_OFFSET_SHIFT 6
#define UCT_GDAKI_DEV_EP_SIZE          64
#define UCT_GDAKI_DEV_QP_SIZE          128

#define UCT_GDAKI_CUDA_REG_FLAGS \
    (CU_MEMHOSTREGISTER_PORTABLE | CU_MEMHOSTREGISTER_DEVICEMAP | \
     CU_MEMHOSTREGISTER_IOMEMORY)

typedef enum {
    UCT_RC_GDAKI_EP_ALLOC_MODE_POOL,
    UCT_RC_GDAKI_EP_ALLOC_MODE_DIRECT,
    UCT_RC_GDAKI_EP_ALLOC_MODE_LAST
} uct_rc_gdaki_ep_alloc_mode_t;

static const char *uct_rc_gdaki_ep_alloc_mode_names[] = {
    [UCT_RC_GDAKI_EP_ALLOC_MODE_POOL]   = "pool",
    [UCT_RC_GDAKI_EP_ALLOC_MODE_DIRECT] = "direct",
    [UCT_RC_GDAKI_EP_ALLOC_MODE_LAST]   = NULL
};

enum {
    UCT_GDAKI_SUPPORTED   = UCS_BIT(0),
    UCT_GDAKI_DMABUF_UMEM = UCS_BIT(1)
};

typedef struct {
    uct_rc_iface_common_config_t      super;
    uct_rc_mlx5_iface_common_config_t mlx5;
    uct_rc_gdaki_ep_alloc_mode_t      ep_alloc_mode;
    unsigned                          num_channels;
    unsigned                          num_eps;
} uct_rc_gdaki_iface_config_t;

ucs_config_field_t uct_rc_gdaki_iface_config_table[] = {
    {UCT_IB_CONFIG_PREFIX, "", NULL,
     ucs_offsetof(uct_rc_gdaki_iface_config_t, super),
     UCS_CONFIG_TYPE_TABLE(uct_rc_iface_common_config_table)},

    {UCT_IB_CONFIG_PREFIX, "", NULL,
     ucs_offsetof(uct_rc_gdaki_iface_config_t, mlx5),
     UCS_CONFIG_TYPE_TABLE(uct_rc_mlx5_common_config_table)},

    {"NUM_CHANNELS", "1",
     "Number of channels. \nRounded up to the next power-of-2 value.\n"
     "Maximum value is 256.",
     ucs_offsetof(uct_rc_gdaki_iface_config_t, num_channels),
     UCS_CONFIG_TYPE_UINT},

    {"EP_ALLOC_MODE", "pool",
     "Endpoint channel allocation mode.\n"
     " - pool   : allocate channels from pooled chunks\n"
     " - direct : allocate channels per endpoint directly",
     ucs_offsetof(uct_rc_gdaki_iface_config_t, ep_alloc_mode),
     UCS_CONFIG_TYPE_ENUM(uct_rc_gdaki_ep_alloc_mode_names)},

    {"NUM_PRE_ALLOC_EPS", "32",
     "Expected number of pre-allocated endpoints. The channel pool grows if\n"
     "more are needed.",
     ucs_offsetof(uct_rc_gdaki_iface_config_t, num_eps), UCS_CONFIG_TYPE_UINT},

    {NULL}
};

static ucs_status_t uct_gdaki_cuda_mem_alloc(size_t size, size_t *granularity_p,
                                             uct_cuda_mem_t *cuda_mem)
{
    CUdevice cu_device;
    ucs_status_t status;

    status = UCT_CUDADRV_FUNC_LOG_ERR(cuCtxGetDevice(&cu_device));
    if (status != UCS_OK) {
        return status;
    }

    return uct_cuda_mem_alloc(UCS_LOG_LEVEL_ERROR, UCS_MEMORY_TYPE_CUDA,
                              UCS_TRY, cu_device, size, granularity_p,
                              cuda_mem);
}

static ucs_status_t uct_rc_gdaki_alloc(size_t size, size_t align,
                                       size_t *granularity_p,
                                       uct_cuda_mem_t *cuda_mem, void **buf_p)
{
    unsigned int flag = 1;
    ucs_status_t status;
    void *buf;

    status = uct_gdaki_cuda_mem_alloc(size + align - 1, granularity_p,
                                      cuda_mem);
    if (status != UCS_OK) {
        return status;
    }

    buf = (void*)ucs_align_up_pow2_ptr(cuda_mem->ptr, align);

    if (!cuda_mem->is_vmm) {
        status = UCT_CUDADRV_FUNC_LOG_ERR(
                cuPointerSetAttribute(&flag, CU_POINTER_ATTRIBUTE_SYNC_MEMOPS,
                                      (CUdeviceptr)buf));
        if (status != UCS_OK) {
            goto err;
        }
    }

    *buf_p = buf;
    return UCS_OK;

err:
    uct_cuda_mem_free(cuda_mem);
    return status;
}

static void uct_rc_gdaki_calc_dev_ep_layout(size_t num_channels, size_t wq_len,
                                            size_t *dev_ep_size_p,
                                            uint64_t *pgsz_bitmap_p)
{
    uint64_t max_page_size;
    size_t wq_umem_offset;

    UCS_STATIC_ASSERT(sizeof(uct_rc_gdaki_dev_ep_t) == UCT_GDAKI_DEV_EP_SIZE);
    UCS_STATIC_ASSERT(sizeof(uct_rc_gdaki_dev_qp_t) == UCT_GDAKI_DEV_QP_SIZE);

    wq_umem_offset = UCT_GDAKI_DEV_EP_SIZE +
                     (UCT_GDAKI_DEV_QP_SIZE * num_channels);
    *dev_ep_size_p = wq_umem_offset + wq_len * num_channels;
    max_page_size  = ucs_min(UCT_GDAKI_DEV_EP_SIZE, UCT_GDAKI_DEV_QP_SIZE)
                     << UCT_GDAKI_CQ_PAGE_OFFSET_SHIFT;
    *pgsz_bitmap_p = (max_page_size << 1) - 1;
}

static int uct_gdaki_check_umem_dmabuf(const uct_ib_md_t *md)
{
    int ret = 0;
#if HAVE_DECL_MLX5DV_UMEM_MASK_DMABUF
    size_t granularity                 = SIZE_MAX;
    struct mlx5dv_devx_umem_in umem_in = {};
    uct_cuda_mem_t cuda_mem;
    ucs_status_t status;
    struct mlx5dv_devx_umem *umem;
    uct_cuda_copy_md_dmabuf_t dmabuf;

    status = uct_gdaki_cuda_mem_alloc(1, &granularity, &cuda_mem);
    if (status != UCS_OK) {
        goto out;
    }

    dmabuf = uct_cuda_copy_md_get_dmabuf((void*)cuda_mem.ptr, 1,
                                         UCS_SYS_DEVICE_ID_UNKNOWN);
    if (dmabuf.fd == UCT_DMABUF_FD_INVALID) {
        goto out_free;
    }

    umem_in.addr        = (void *)(uintptr_t)dmabuf.offset;
    umem_in.size        = 1;
    umem_in.access      = IBV_ACCESS_LOCAL_WRITE;
    umem_in.pgsz_bitmap = UINT64_MAX & ~(ucs_get_page_size() - 1);
    umem_in.comp_mask   = MLX5DV_UMEM_MASK_DMABUF;
    umem_in.dmabuf_fd   = dmabuf.fd;

    umem = mlx5dv_devx_umem_reg_ex(md->dev.ibv_context, &umem_in);
    if (umem == NULL) {
        goto out_close;
    }

    ret = 1;
out_dereg:
    mlx5dv_devx_umem_dereg(umem);
out_close:
    ucs_close_fd(&dmabuf.fd);
out_free:
    uct_cuda_mem_free(&cuda_mem);
out:
#endif
    return ret;
}

static int uct_gdaki_is_dmabuf_supported(const uct_ib_md_t *md)
{
    static int dmabuf_supported = -1;
    int loglevel                = (md->config.gda_dmabuf_enable == UCS_YES) ?
                                                         UCS_LOG_LEVEL_DIAG :
                                                         UCS_LOG_LEVEL_DEBUG;

    if (dmabuf_supported != -1) {
        return dmabuf_supported;
    }

    if (!(md->cap_flags & UCT_MD_FLAG_REG_DMABUF)) {
        dmabuf_supported = 0;
        ucs_log(loglevel, "IB doesn't support DMA-BUF");
    } else if (!uct_cuda_copy_md_is_dmabuf_supported()) {
        dmabuf_supported = 0;
        ucs_log(loglevel, "CUDA doesn't support DMA-BUF");
    } else if (!uct_gdaki_check_umem_dmabuf(md)) {
        dmabuf_supported = 0;
        ucs_log(loglevel, "DEVX UMEM doesn't support DMA-BUF");
    } else {
        dmabuf_supported = 1;
    }

    return dmabuf_supported;
}

static int uct_gdaki_md_check_uar(const uct_ib_md_t *md)
{
    int ret = 0;
    struct mlx5dv_devx_uar *uar;
    ucs_status_t status;

    uar = mlx5dv_devx_alloc_uar(md->dev.ibv_context,
                                UCT_IB_MLX5_UAR_ALLOC_TYPE_WC);
    if (uar == NULL) {
        uar = mlx5dv_devx_alloc_uar(md->dev.ibv_context,
                                    UCT_IB_MLX5_UAR_ALLOC_TYPE_NC);
    }

    if (uar == NULL) {
        return 0;
    }

    status = UCT_CUDADRV_FUNC_LOG_DEBUG(
            cuMemHostRegister(uar->reg_addr, UCT_IB_MLX5_BF_REG_SIZE,
                              UCT_GDAKI_CUDA_REG_FLAGS));
    if (status != UCS_OK) {
        goto out;
    }

    UCT_CUDADRV_FUNC_LOG_WARN(cuMemHostUnregister(uar->reg_addr));
    ret = 1;

out:
    mlx5dv_devx_free_uar(uar);
    return ret;
}

static CUdevice uct_gdaki_push_primary_ctx(int retain_inactive_ctx)
{
    CUdevice cuda_dev;
    ucs_status_t status;

    status = uct_cuda_ctx_primary_push_first_active(&cuda_dev);
    if (status == UCS_OK) {
        return cuda_dev;
    }

    if ((status != UCS_ERR_NO_DEVICE) || !retain_inactive_ctx) {
        if (status == UCS_ERR_NO_DEVICE) {
            ucs_diag("no active primary CUDA context on any device. Please set "
                     "UCX_IB_GDA_RETAIN_INACTIVE_CTX=yes to retain inactive "
                     "context.");
        }
        return CU_DEVICE_INVALID;
    }

    status = UCT_CUDADRV_FUNC_LOG_ERR(cuDeviceGet(&cuda_dev, 0));
    if (status != UCS_OK) {
        return CU_DEVICE_INVALID;
    }

    status = uct_cuda_ctx_primary_push(cuda_dev, 1, UCS_LOG_LEVEL_ERROR);
    if (status != UCS_OK) {
        return CU_DEVICE_INVALID;
    }

    return cuda_dev;
}

static uint32_t
uct_gdaki_check_cuda_ctx_dependent_features(const uct_ib_md_t *md)
{
    uint32_t features = 0;
    CUdevice cuda_dev;

    cuda_dev = uct_gdaki_push_primary_ctx(md->config.gda_retain_inactive_ctx);
    if (cuda_dev == CU_DEVICE_INVALID) {
        return features;
    }

    if (!uct_gdaki_md_check_uar(md)) {
        ucs_diag("GDAKI not supported, please add NVreg_RegistryDwords="
                 "\"PeerMappingOverride=1;\" option for nvidia kernel driver");
        goto out;
    }

    if ((md->config.gda_dmabuf_enable != UCS_NO) &&
        uct_gdaki_is_dmabuf_supported(md)) {
        features |= UCT_GDAKI_DMABUF_UMEM;
        ucs_debug("%s: using dmabuf for gda transport",
                  uct_ib_device_name(&md->dev));
    } else if ((md->config.gda_dmabuf_enable != UCS_YES) &&
               (md->reg_mem_types & UCS_BIT(UCS_MEMORY_TYPE_CUDA))) {
        ucs_debug("%s: using peermem for gda transport",
                  uct_ib_device_name(&md->dev));
    } else {
        if (md->config.gda_dmabuf_enable != UCS_YES) {
            ucs_diag("GDAKI not supported, please load Nvidia peermem driver "
                     "by running \"modprobe nvidia_peermem\"");
        }
        goto out;
    }

    features |= UCT_GDAKI_SUPPORTED;

out:
    uct_cuda_ctx_primary_pop_and_release(cuda_dev);
    return features;
}

static uint32_t uct_gdaki_get_driver_features(const uct_ib_md_t *md)
{
    static ucs_init_once_t once = UCS_INIT_ONCE_INITIALIZER;
    static uint32_t features;

    UCS_INIT_ONCE(&once) {
        features = uct_gdaki_check_cuda_ctx_dependent_features(md);
    }

    return features;
}

static struct mlx5dv_devx_umem *
uct_rc_gdaki_umem_reg(const uct_ib_md_t *md, struct ibv_context *ibv_context,
                      void *address, size_t length, uint64_t pgsz_bitmap)
{
#if HAVE_DECL_MLX5DV_UMEM_MASK_DMABUF
    struct mlx5dv_devx_umem_in umem_in = {};
    uct_cuda_copy_md_dmabuf_t dmabuf   = {
        .fd     = UCT_DMABUF_FD_INVALID,
        .offset = 0
    };
    struct mlx5dv_devx_umem *umem;

    umem_in.size        = length;
    umem_in.access      = IBV_ACCESS_LOCAL_WRITE;
    umem_in.pgsz_bitmap = pgsz_bitmap;

    if (uct_gdaki_get_driver_features(md) & UCT_GDAKI_DMABUF_UMEM) {
        dmabuf = uct_cuda_copy_md_get_dmabuf(address, length,
                                             UCS_SYS_DEVICE_ID_UNKNOWN);
    }

    if (dmabuf.fd == UCT_DMABUF_FD_INVALID) {
        umem_in.addr      = address;
    } else {
        umem_in.comp_mask = MLX5DV_UMEM_MASK_DMABUF;
        umem_in.dmabuf_fd = dmabuf.fd;
        umem_in.addr      = (void *)(uintptr_t)dmabuf.offset;
    }

    umem = mlx5dv_devx_umem_reg_ex(ibv_context, &umem_in);
    if (dmabuf.fd != UCT_DMABUF_FD_INVALID) {
        ucs_close_fd(&dmabuf.fd);
    }

    return umem;
#else
    return mlx5dv_devx_umem_reg(ibv_context, address, length,
                                IBV_ACCESS_LOCAL_WRITE);
#endif
}

static ucs_status_t
uct_rc_gdaki_init_umem(uct_rc_gdaki_iface_t *iface, uint64_t pgsz_bitmap,
                       size_t mem_size, uct_rc_gdaki_channel_block_mem_t *mem)
{
    uct_ib_md_t *md = ucs_derived_of(iface->super.super.super.super.md,
                                     uct_ib_md_t);
    ucs_status_t status;

    status = UCT_CUDADRV_FUNC_LOG_ERR(cuCtxPushCurrent(iface->cuda.ctx));
    if (status != UCS_OK) {
        return status;
    }

    status = uct_rc_gdaki_alloc(mem_size, ucs_get_page_size(),
                                &iface->cuda.alloc_granularity, &mem->gpu_raw,
                                &mem->gpu_mem);
    if (status != UCS_OK) {
        goto out_ctx;
    }

    mem->umem = uct_rc_gdaki_umem_reg(md, md->dev.ibv_context, mem->gpu_mem,
                                      mem_size, pgsz_bitmap);
    if (mem->umem == NULL) {
        uct_ib_check_memlock_limit_msg(md->dev.ibv_context, UCS_LOG_LEVEL_ERROR,
                                       "mlx5dv_devx_umem_reg(ptr=%p size=%zu)",
                                       mem->gpu_mem, mem_size);
        status = UCS_ERR_NO_MEMORY;
        goto err_umem;
    }

    (void)UCT_CUDADRV_FUNC_LOG_WARN(cuCtxPopCurrent(NULL));
    return UCS_OK;

err_umem:
    uct_cuda_mem_free(&mem->gpu_raw);
    mem->gpu_mem = NULL;
out_ctx:
    (void)UCT_CUDADRV_FUNC_LOG_WARN(cuCtxPopCurrent(NULL));
    return status;
}

typedef struct {
    uint64_t pgsz_bitmap;
    size_t   dev_ep_size;
} uct_rc_gdaki_pool_priv_t;

static uct_rc_gdaki_channel_block_t *
uct_rc_gdaki_channel_block(uct_rc_gdaki_iface_t *iface, ucs_mpool_t *mp,
                           void *elems, unsigned ep_index)
{
    if (iface->ep_alloc_mode == UCT_RC_GDAKI_EP_ALLOC_MODE_POOL) {
        return ucs_mpool_chunk_obj(mp, elems, ep_index);
    } else {
        return elems;
    }
}

static void
uct_rc_gdaki_channel_block_reset_qps(uct_rc_gdaki_iface_t *iface,
                                     uct_rc_gdaki_channel_block_t *block)
{
    uct_ib_iface_t *ib_iface = &iface->super.super.super;
    uct_rc_gdaki_channel_t *channel;
    ucs_status_t status;
    unsigned i;

    for (i = 0; i < iface->num_channels; i++) {
        channel = &block->channels[i];

        status = uct_ib_mlx5_modify_qp_state(ib_iface, &channel->qp.super,
                                             IBV_QPS_RESET);
        if (status != UCS_OK) {
            ucs_fatal("failed to reset gdaki qp 0x%x: %s",
                      channel->qp.super.qp_num, ucs_status_string(status));
        }

        status = uct_ib_mlx5_devx_qp_rst2init(ib_iface, &channel->qp.super,
                                              IBV_QPT_RC);
        if (status != UCS_OK) {
            ucs_fatal("failed to move gdaki qp 0x%x to init: %s",
                      channel->qp.super.qp_num, ucs_status_string(status));
        }
    }
}

static int uct_rc_gdaki_devx_uar_cmp(uct_rc_gdaki_devx_uar_t *wrap,
                                     uct_priv_worker_t *worker,
                                     uct_ib_mlx5_md_t *md)
{
    return (wrap->uar->ctx == md->super.dev.ibv_context) &&
           (wrap->uar->super.mode == UCT_IB_MLX5_MMIO_MODE_DB);
}

static void uct_rc_gdaki_devx_uar_cleanup(uct_rc_gdaki_devx_uar_t *wrap)
{
    (void)UCT_CUDADRV_FUNC_LOG_WARN(
            cuMemHostUnregister(wrap->uar->super.addr.ptr));
    uct_worker_tl_data_put(wrap->uar, uct_ib_mlx5_devx_uar_cleanup);
}

static ucs_status_t uct_rc_gdaki_devx_uar_init(uct_rc_gdaki_devx_uar_t *wrap,
                                               uct_priv_worker_t *worker,
                                               uct_ib_mlx5_md_t *md)
{
    ucs_status_t status;

    wrap->uar = uct_worker_tl_data_get(worker, UCT_IB_MLX5_DEVX_UAR_KEY,
                                       uct_ib_mlx5_devx_uar_t,
                                       uct_ib_mlx5_devx_uar_cmp,
                                       uct_ib_mlx5_devx_uar_init, md,
                                       UCT_IB_MLX5_MMIO_MODE_DB);
    if (UCS_PTR_IS_ERR(wrap->uar)) {
        return UCS_PTR_STATUS(wrap->uar);
    }

    (void)UCT_CUDADRV_FUNC_LOG_WARN(
            cuMemHostRegister(wrap->uar->super.addr.ptr,
                              UCT_IB_MLX5_BF_REG_SIZE * 2,
                              UCT_GDAKI_CUDA_REG_FLAGS));
    status = UCT_CUDADRV_FUNC_LOG_ERR(
            cuMemHostGetDevicePointer(&wrap->device_ptr,
                                      wrap->uar->super.addr.ptr, 0));
    if (status != UCS_OK) {
        goto out_unreg;
    }

    return UCS_OK;

out_unreg:
    uct_rc_gdaki_devx_uar_cleanup(wrap);
    return status;
}

static void
uct_rc_gdaki_chunk_channels_destroy(uct_rc_gdaki_iface_t *iface,
                                    ucs_mpool_t *mp, void *elems,
                                    unsigned num_elems, unsigned end_ep,
                                    unsigned end_ch)
{
    uct_rc_gdaki_channel_block_t *channel_block;
    uct_rc_gdaki_channel_t *channel;
    unsigned ep_index, channel_index, num_channels;

    for (ep_index = 0; ep_index < num_elems; ++ep_index) {
        channel_block = uct_rc_gdaki_channel_block(iface, mp, elems, ep_index);
        num_channels  = (ep_index < end_ep) ? end_ch + 1 : end_ch;
        for (channel_index = 0; channel_index < num_channels; ++channel_index) {
            channel = &channel_block->channels[channel_index];
            uct_ib_mlx5_devx_destroy_qp_common(&channel->qp.super);
            uct_ib_mlx5_qp_mmio_cleanup(&channel->qp.super, channel->qp.reg);
            uct_ib_mlx5_devx_destroy_cq_common(&channel->cq);
        }
    }
}

/*
 * The device owns the channel CQs and polls them, so the host learns about a
 * failed QP from a CQ event. Each channel CQ is subscribed to the iface error
 * event channel with its CQ number as the cookie.
 */
static ucs_status_t
uct_rc_gdaki_iface_subscribe_cq(uct_rc_gdaki_iface_t *iface,
                                uct_ib_mlx5_cq_t *cq)
{
    uint16_t event_num = 0; /* Completion event */
    int ret;

    if (iface->err_event_channel == NULL) {
        return UCS_OK;
    }

    ret = mlx5dv_devx_subscribe_devx_event(iface->err_event_channel,
                                           cq->devx.obj, sizeof(event_num),
                                           &event_num, cq->cq_num);
    if (ret != 0) {
        ucs_error("mlx5dv_devx_subscribe_devx_event(CQ 0x%x) failed: %m",
                  cq->cq_num);
        return UCS_ERR_IO_ERROR;
    }

    return UCS_OK;
}

/*
 * Arm a channel CQ to raise an event for solicited completions only. Device
 * operations are never solicited, so only an error completion raises the
 * event. The doorbell record lives in device memory and is not needed for
 * arming, only the doorbell is written.
 */
static void uct_rc_gdaki_cq_arm_solicited(uct_ib_mlx5_cq_t *cq)
{
    uint32_t sn_ci_cmd = ((cq->cq_sn & 3) << 28) | MLX5_CQ_DB_REQ_NOT_SOL;
    uint64_t doorbell  = ((uint64_t)sn_ci_cmd << 32) | cq->cq_num;

    ucs_memory_cpu_store_fence();
    *(volatile uint64_t*)UCS_PTR_BYTE_OFFSET(cq->uar, MLX5_CQ_DOORBELL) =
            htobe64(doorbell);
    ucs_memory_bus_store_fence();
    ++cq->cq_sn;
}

static ucs_status_t
uct_rc_gdaki_init_channel_chunk(uct_rc_gdaki_iface_t *iface,
                                uct_rc_gdaki_channel_block_mem_t *mem,
                                size_t dev_ep_size, ucs_mpool_t *mp,
                                void *elems, unsigned num_elems)
{
    uct_ib_iface_init_attr_t init_attr = {};
    uct_ib_mlx5_cq_attr_t cq_attr      = {};
    uct_ib_mlx5_qp_attr_t qp_attr      = {};
    uct_ib_mlx5_dbrec_t dbrec;
    size_t ep_offset, ep_qp_offset, ep_wq_offset;
    unsigned ep_index, channel_index;
    uct_rc_gdaki_channel_block_t *channel_block;
    uct_rc_gdaki_channel_t *channel;
    ucs_status_t status;

    init_attr.cq_len[UCT_IB_DIR_TX] = 1;
    uct_ib_mlx5_cq_calc_sizes(&iface->super.super.super, UCT_IB_DIR_TX,
                              &init_attr, 0, &cq_attr);
    uct_rc_iface_fill_attr(&iface->super.super, &qp_attr.super,
                           iface->super.super.config.tx_qp_len, 0, NULL);

    cq_attr.flags                           |= UCT_IB_MLX5_CQ_IGNORE_OVERRUN;
    qp_attr.mmio_mode                        = UCT_IB_MLX5_MMIO_MODE_DB;
    qp_attr.bf_copy_mode                     = UCT_IB_MLX5_BF_COPY_MODE_GENERIC;
    qp_attr.super.srq_num                    = 0;
    qp_attr.super.max_inl_cqe[UCT_IB_DIR_TX] = 0;
    uct_ib_mlx5_wq_calc_sizes(&qp_attr);

    ep_qp_offset = UCT_GDAKI_DEV_EP_SIZE;
    ep_wq_offset = ep_qp_offset + iface->num_channels * UCT_GDAKI_DEV_QP_SIZE;
    dbrec.mem_id = mem->umem->umem_id;

    for (channel_index = 0; channel_index < iface->num_channels;
         ++channel_index) {
        size_t ch_cq_offset = channel_index * UCT_GDAKI_DEV_QP_SIZE;
        size_t ch_wq_offset = channel_index * qp_attr.len;
        for (ep_index = 0; ep_index < num_elems; ++ep_index) {
            ep_offset     = ep_index * dev_ep_size;
            channel_block = uct_rc_gdaki_channel_block(iface, mp, elems,
                                                       ep_index);
            channel       = &channel_block->channels[channel_index];
            if (channel_index == 0) {
                channel_block->gpu_ptr = (uintptr_t)
                        UCS_PTR_BYTE_OFFSET(mem->gpu_mem, ep_offset);
            }

            channel->cq.devx.mem.mem       = mem->umem;
            channel->qp.super.devx.mem.mem = mem->umem;

            cq_attr.umem_offset    = ep_offset + ep_qp_offset + ch_cq_offset;
            dbrec.offset           = cq_attr.umem_offset +
                                     ucs_offsetof(uct_rc_gdaki_dev_qp_t, cq_dbrec);
            channel->cq.devx.dbrec = &dbrec;
            status = uct_ib_mlx5_devx_create_cq_common(&iface->super.super.super,
                                                       UCT_IB_DIR_TX, &cq_attr,
                                                       &channel->cq, 0, 0);
            if (status != UCS_OK) {
                goto err_cleanup;
            }

            status = uct_rc_gdaki_iface_subscribe_cq(iface, &channel->cq);
            if (status != UCS_OK) {
                uct_ib_mlx5_devx_destroy_cq_common(&channel->cq);
                goto err_cleanup;
            }

            qp_attr.umem_offset = ep_offset + ep_wq_offset + ch_wq_offset;
            dbrec.offset        = cq_attr.umem_offset +
                                  ucs_offsetof(uct_rc_gdaki_dev_qp_t, qp_dbrec);
            channel->qp.super.devx.dbrec = &dbrec;
            status                       = uct_ib_mlx5_devx_create_qp_common(
                    &iface->super.super.super, &channel->cq, &channel->cq,
                    &channel->qp.super, &channel->qp, &qp_attr);
            if (status != UCS_OK) {
                uct_ib_mlx5_devx_destroy_cq_common(&channel->cq);
                goto err_cleanup;
            }
        }
    }

    return UCS_OK;

err_cleanup:
    uct_rc_gdaki_chunk_channels_destroy(iface, mp, elems, num_elems, ep_index,
                                        channel_index);
    ucs_error("gdaki channel chunk init: create_cq/qp failed (ch=%u ep=%u)",
              channel_index, ep_index);
    return status;
}

static ucs_status_t
uct_rc_gdaki_pool_chunk_alloc(ucs_mpool_t *mp, size_t *size_p, void **chunk_p)
{
    uct_rc_gdaki_pool_priv_t *priv = ucs_mpool_priv(mp);
    uct_rc_gdaki_iface_t *iface    = ucs_container_of(mp, uct_rc_gdaki_iface_t,
                                                      channel_pool);
    uct_rc_gdaki_channel_block_mem_t *hdr;
    size_t host_alloc_size, gpu_alloc_size;
    unsigned num_elems;
    ucs_status_t status;

    host_alloc_size = sizeof(*hdr) + *size_p;
    hdr             = ucs_malloc(host_alloc_size, "gdaki pool chunk");
    if (hdr == NULL) {
        return UCS_ERR_NO_MEMORY;
    }

    *chunk_p  = hdr + 1;
    num_elems = ucs_mpool_num_elems_per_chunk(mp, *chunk_p, *size_p);
    if (num_elems == 0) {
        status = UCS_ERR_NO_MEMORY;
        goto err_free_hdr;
    }

    gpu_alloc_size = num_elems * priv->dev_ep_size;
    status = uct_rc_gdaki_init_umem(iface, priv->pgsz_bitmap, gpu_alloc_size,
                                    hdr);
    if (status != UCS_OK) {
        goto err_free_hdr;
    }

    status = uct_rc_gdaki_init_channel_chunk(
            iface, hdr, priv->dev_ep_size, mp,
            ucs_mpool_chunk_elems(mp, (void*)(hdr + 1)), num_elems);
    if (status != UCS_OK) {
        goto err_umem;
    }

    return UCS_OK;

err_umem:
    mlx5dv_devx_umem_dereg(hdr->umem);
    uct_cuda_mem_free(&hdr->gpu_raw);
err_free_hdr:
    ucs_free(hdr);
    return status;
}


static void uct_rc_gdaki_pool_chunk_release(ucs_mpool_t *mp, void *chunk)
{
    uct_rc_gdaki_iface_t *iface = ucs_container_of(mp, uct_rc_gdaki_iface_t,
                                                   channel_pool);
    uct_rc_gdaki_channel_block_mem_t *hdr =
            (uct_rc_gdaki_channel_block_mem_t*)chunk - 1;
    ucs_mpool_chunk_t *mp_chunk = chunk;

    uct_rc_gdaki_chunk_channels_destroy(iface, mp, mp_chunk->elems,
                                        mp_chunk->num_elems,
                                        mp_chunk->num_elems,
                                        iface->num_channels - 1);
    mlx5dv_devx_umem_dereg(hdr->umem);
    uct_cuda_mem_free(&hdr->gpu_raw);
    ucs_free(hdr);
}

static ucs_mpool_ops_t uct_rc_gdaki_pool_mpool_ops = {
    .chunk_alloc   = uct_rc_gdaki_pool_chunk_alloc,
    .chunk_release = uct_rc_gdaki_pool_chunk_release,
    .obj_init      = NULL,
    .obj_cleanup   = NULL,
    .obj_str       = NULL,
};

static ucs_status_t
uct_rc_gdaki_iface_init_channel_pool(uct_rc_gdaki_iface_t *iface,
                                     const uct_rc_gdaki_iface_config_t *config)
{
    uct_ib_mlx5_qp_attr_t qp_attr = {};
    ucs_mpool_params_t mp_params;
    uct_rc_gdaki_pool_priv_t *priv;
    ucs_status_t status;

    ucs_mpool_params_reset(&mp_params);
    mp_params.priv_size       = sizeof(uct_rc_gdaki_pool_priv_t);
    mp_params.elem_size       = sizeof(uct_rc_gdaki_channel_block_t) +
                                (sizeof(uct_rc_gdaki_channel_t) *
                                 iface->num_channels);
    mp_params.alignment       = UCS_SYS_CACHE_LINE_SIZE;
    mp_params.elems_per_chunk = config->num_eps;
    mp_params.max_elems       = UINT_MAX;
    mp_params.ops             = &uct_rc_gdaki_pool_mpool_ops;
    mp_params.name            = "gdaki_channel_pool";

    status = ucs_mpool_init(&mp_params, &iface->channel_pool);
    if (status != UCS_OK) {
        return status;
    }

    uct_rc_iface_fill_attr(&iface->super.super, &qp_attr.super,
                           iface->super.super.config.tx_qp_len, 0, NULL);
    uct_ib_mlx5_wq_calc_sizes(&qp_attr);

    priv = ucs_mpool_priv(&iface->channel_pool);
    uct_rc_gdaki_calc_dev_ep_layout(iface->num_channels, qp_attr.len,
                                    &priv->dev_ep_size, &priv->pgsz_bitmap);

    return UCS_OK;
}

static void uct_rc_gdaki_iface_cleanup_channel_pool(uct_rc_gdaki_iface_t *iface)
{
    ucs_mpool_t *mp = &iface->channel_pool;

    if (mp->data == NULL) {
        return;
    }
    ucs_mpool_cleanup(mp, 0);
}

static ucs_status_t
uct_rc_gdaki_ep_init_channels_pooled(uct_rc_gdaki_iface_t *iface,
                                     uct_rc_gdaki_ep_t *ep)
{
    ep->channel_block = ucs_mpool_get(&iface->channel_pool);

    if (ep->channel_block == NULL) {
        return UCS_ERR_NO_MEMORY;
    }

    return UCS_OK;
}

static UCS_F_ALWAYS_INLINE void
uct_rc_gdaki_ep_reset_channels(uct_rc_gdaki_ep_t *ep)
{
    ep->mem.umem      = NULL;
    ep->mem.gpu_mem   = NULL;
    ep->channel_block = NULL;
}

static void uct_rc_gdaki_cleanup_channels_pooled(uct_rc_gdaki_iface_t *iface,
                                                 uct_rc_gdaki_ep_t *ep)
{
    if (ep->channel_block == NULL) {
        return;
    }

    uct_rc_gdaki_channel_block_reset_qps(iface, ep->channel_block);
    ucs_mpool_put(ep->channel_block);
    uct_rc_gdaki_ep_reset_channels(ep);
}

static ucs_status_t
uct_rc_gdaki_ep_init_channels_direct(uct_rc_gdaki_iface_t *iface,
                                     uct_rc_gdaki_ep_t *ep)
{
    uct_ib_mlx5_qp_attr_t qp_attr = {};
    size_t channel_block_size     = sizeof(uct_rc_gdaki_channel_block_t) +
                                    (iface->num_channels *
                                     sizeof(uct_rc_gdaki_channel_t));
    uint64_t pgsz_bitmap;
    size_t dev_ep_size;
    ucs_status_t status;

    uct_rc_iface_fill_attr(&iface->super.super, &qp_attr.super,
                           iface->super.super.config.tx_qp_len, 0, NULL);
    uct_ib_mlx5_wq_calc_sizes(&qp_attr);
    uct_rc_gdaki_calc_dev_ep_layout(iface->num_channels, qp_attr.len,
                                    &dev_ep_size, &pgsz_bitmap);

    ep->channel_block = ucs_calloc(1, channel_block_size, "gdaki channels");
    if (ep->channel_block == NULL) {
        return UCS_ERR_NO_MEMORY;
    }

    status = uct_rc_gdaki_init_umem(iface, pgsz_bitmap, dev_ep_size, &ep->mem);
    if (status != UCS_OK) {
        goto err_block;
    }

    ep->channel_block->gpu_ptr = (uintptr_t)ep->mem.gpu_mem;

    status = uct_rc_gdaki_init_channel_chunk(iface, &ep->mem, dev_ep_size, NULL,
                                             ep->channel_block, 1);
    if (status != UCS_OK) {
        goto err_umem;
    }

    return UCS_OK;

err_umem:
    mlx5dv_devx_umem_dereg(ep->mem.umem);
    uct_cuda_mem_free(&ep->mem.gpu_raw);
err_block:
    ucs_free(ep->channel_block);
    uct_rc_gdaki_ep_reset_channels(ep);
    return status;
}

static void uct_rc_gdaki_cleanup_channels_direct(uct_rc_gdaki_iface_t *iface,
                                                 uct_rc_gdaki_ep_t *ep)
{
    if (ep->channel_block == NULL) {
        return;
    }

    uct_rc_gdaki_chunk_channels_destroy(iface, NULL, ep->channel_block, 1, 1,
                                        iface->num_channels - 1);
    mlx5dv_devx_umem_dereg(ep->mem.umem);
    uct_cuda_mem_free(&ep->mem.gpu_raw);
    ucs_free(ep->channel_block);
    uct_rc_gdaki_ep_reset_channels(ep);
}

static ucs_status_t uct_rc_gdaki_ep_init_channels(uct_rc_gdaki_iface_t *iface,
                                                  uct_rc_gdaki_ep_t *ep)
{
    if (iface->ep_alloc_mode == UCT_RC_GDAKI_EP_ALLOC_MODE_POOL) {
        return uct_rc_gdaki_ep_init_channels_pooled(iface, ep);
    }

    return uct_rc_gdaki_ep_init_channels_direct(iface, ep);
}

static void uct_rc_gdaki_ep_cleanup_channels(uct_rc_gdaki_iface_t *iface,
                                             uct_rc_gdaki_ep_t *ep)
{
    if (iface->ep_alloc_mode == UCT_RC_GDAKI_EP_ALLOC_MODE_POOL) {
        uct_rc_gdaki_cleanup_channels_pooled(iface, ep);
        return;
    }

    uct_rc_gdaki_cleanup_channels_direct(iface, ep);
}

static ucs_status_t
uct_rc_gdaki_iface_cuda_ctx_init(uct_rc_gdaki_iface_t *iface)
{
    uct_ib_iface_t *ib_iface = &iface->super.super.super;
    CUcontext cuda_ctx;
    ucs_status_t status;

    if (iface->cuda.ctx != NULL) {
        return UCS_OK;
    }

    status = UCT_CUDADRV_FUNC_LOG_ERR(
            cuDevicePrimaryCtxRetain(&cuda_ctx, iface->cuda.dev));
    if (status != UCS_OK) {
        return status;
    }

    status = UCT_CUDADRV_FUNC_LOG_ERR(cuCtxPushCurrent(cuda_ctx));
    if (status != UCS_OK) {
        goto err_release_ctx;
    }

    /* Register the shared UAR with CUDA under the retained context */
    iface->uar_wrap = uct_worker_tl_data_get(
            ib_iface->super.worker, UCT_RC_GDAKI_DEVX_UAR_KEY,
            uct_rc_gdaki_devx_uar_t, uct_rc_gdaki_devx_uar_cmp,
            uct_rc_gdaki_devx_uar_init, ib_iface->super.worker,
            ucs_derived_of(ib_iface->super.md, uct_ib_mlx5_md_t));
    if (UCS_PTR_IS_ERR(iface->uar_wrap)) {
        status          = UCS_PTR_STATUS(iface->uar_wrap);
        iface->uar_wrap = NULL;
        goto err_pop_ctx;
    }

    (void)uct_cuda_copy_set_ctx_sync_memops(UCS_LOG_LEVEL_WARN);
    (void)UCT_CUDADRV_FUNC_LOG_WARN(cuCtxPopCurrent(NULL));
    iface->cuda.ctx = cuda_ctx;
    return UCS_OK;

err_pop_ctx:
    (void)UCT_CUDADRV_FUNC_LOG_WARN(cuCtxPopCurrent(NULL));
err_release_ctx:
    (void)UCT_CUDADRV_FUNC_LOG_WARN(cuDevicePrimaryCtxRelease(iface->cuda.dev));
    return status;
}

/* mlx5 QP state of a QP in error */
#define UCT_RC_GDAKI_QP_STATE_ERR 6

/* The failover buffer holds the copies of two work queues, of a failed
 * endpoint and of the endpoint it moves to, and room for small copies */
#define UCT_RC_GDAKI_FAILOVER_BUF_EXTRA 4096

static ucs_status_t uct_rc_gdaki_iface_failover_init(uct_rc_gdaki_iface_t *iface)
{
    size_t wqe_num, buf_size;
    ucs_status_t status;
    CUstream stream;
    void *buf;

    if (iface->failover.stream != NULL) {
        return UCS_OK;
    }

    wqe_num  = uct_ib_mlx5_devx_sq_length(iface->super.super.config.tx_qp_len);
    buf_size = (2 * (wqe_num << DOCA_GPUNETIO_IB_MLX5_WQE_SQ_SHIFT)) +
               UCT_RC_GDAKI_FAILOVER_BUF_EXTRA;

    status = UCT_CUDADRV_FUNC_LOG_ERR(cuCtxPushCurrent(iface->cuda.ctx));
    if (status != UCS_OK) {
        return status;
    }

    status = UCT_CUDADRV_FUNC_LOG_ERR(
            cuStreamCreate(&stream, CU_STREAM_NON_BLOCKING));
    if (status != UCS_OK) {
        goto out_pop;
    }

    status = UCT_CUDADRV_FUNC_LOG_ERR(cuMemHostAlloc(&buf, buf_size, 0));
    if (status != UCS_OK) {
        (void)UCT_CUDADRV_FUNC_LOG_WARN(cuStreamDestroy(stream));
        goto out_pop;
    }

    iface->failover.stream   = stream;
    iface->failover.buf      = buf;
    iface->failover.buf_size = buf_size;

out_pop:
    (void)UCT_CUDADRV_FUNC_LOG_WARN(cuCtxPopCurrent(NULL));
    return status;
}

static void uct_rc_gdaki_iface_failover_cleanup(uct_rc_gdaki_iface_t *iface)
{
    if (iface->failover.stream == NULL) {
        return;
    }

    if (UCT_CUDADRV_FUNC_LOG_WARN(cuCtxPushCurrent(iface->cuda.ctx)) !=
        UCS_OK) {
        return;
    }

    (void)UCT_CUDADRV_FUNC_LOG_WARN(cuMemFreeHost(iface->failover.buf));
    (void)UCT_CUDADRV_FUNC_LOG_WARN(cuStreamDestroy(iface->failover.stream));
    (void)UCT_CUDADRV_FUNC_LOG_WARN(cuCtxPopCurrent(NULL));
    iface->failover.stream = NULL;
}

/*
 * Copy between the failover buffer and device memory. Only an asynchronous
 * copy on a non-blocking stream is used, since anything else waits for the
 * running kernels, which wait for the failover.
 */
static ucs_status_t
uct_rc_gdaki_failover_copy(uct_rc_gdaki_iface_t *iface, int to_device,
                           CUdeviceptr dev_addr, void *host_addr,
                           size_t length)
{
    ucs_status_t status;

    status = UCT_CUDADRV_FUNC_LOG_ERR(cuCtxPushCurrent(iface->cuda.ctx));
    if (status != UCS_OK) {
        return status;
    }

    if (to_device) {
        status = UCT_CUDADRV_FUNC_LOG_ERR(
                cuMemcpyHtoDAsync(dev_addr, host_addr, length,
                                  iface->failover.stream));
    } else {
        status = UCT_CUDADRV_FUNC_LOG_ERR(
                cuMemcpyDtoHAsync(host_addr, dev_addr, length,
                                  iface->failover.stream));
    }

    if (status == UCS_OK) {
        status = UCT_CUDADRV_FUNC_LOG_ERR(
                cuStreamSynchronize(iface->failover.stream));
    }

    (void)UCT_CUDADRV_FUNC_LOG_WARN(cuCtxPopCurrent(NULL));
    return status;
}

/* Room for small copies in the failover buffer */
static void *uct_rc_gdaki_failover_scratch(uct_rc_gdaki_iface_t *iface)
{
    return UCS_PTR_BYTE_OFFSET(iface->failover.buf,
                               iface->failover.buf_size -
                               UCT_RC_GDAKI_FAILOVER_BUF_EXTRA);
}

static CUdeviceptr uct_rc_gdaki_ep_dev_qp_addr(uct_rc_gdaki_ep_t *ep,
                                               unsigned ch)
{
    return (CUdeviceptr)(ep->channel_block->gpu_ptr + UCT_GDAKI_DEV_EP_SIZE +
                         (ch * UCT_GDAKI_DEV_QP_SIZE));
}

static ucs_status_t
uct_rc_gdaki_failover_read_dev_qp(uct_rc_gdaki_iface_t *iface,
                                  uct_rc_gdaki_ep_t *ep, unsigned ch,
                                  uct_rc_gdaki_dev_qp_t **dev_qp_p)
{
    void *buf = uct_rc_gdaki_failover_scratch(iface);
    ucs_status_t status;

    status = uct_rc_gdaki_failover_copy(iface, 0,
                                        uct_rc_gdaki_ep_dev_qp_addr(ep, ch),
                                        buf, sizeof(uct_rc_gdaki_dev_qp_t));
    *dev_qp_p = buf;
    return status;
}

/*
 * Query the context of the QP of a channel to out, which is query_qp_out, with
 * the context extension when the device reports the error syndrome there
 */
static ucs_status_t
uct_rc_gdaki_channel_query(uct_rc_gdaki_iface_t *iface,
                           uct_rc_gdaki_channel_t *channel, void *out)
{
    uct_ib_mlx5_md_t *md = uct_ib_mlx5_iface_md(&iface->super.super.super);
    char in[UCT_IB_MLX5DV_ST_SZ_BYTES(query_qp_in)] = {};

    if (md->flags & UCT_IB_MLX5_MD_FLAG_QP_ERROR_SYNDROME) {
        UCT_IB_MLX5DV_SET(query_qp_in, in, qpc_ext, 1);
    }

    return uct_ib_mlx5_devx_query_qp(&channel->qp.super, in, sizeof(in), out,
                                     UCT_IB_MLX5DV_ST_SZ_BYTES(query_qp_out));
}

static ucs_status_t
uct_rc_gdaki_channel_query_state(uct_rc_gdaki_iface_t *iface,
                                 uct_rc_gdaki_channel_t *channel,
                                 unsigned *state_p, uint32_t *next_rcv_psn_p)
{
    char out[UCT_IB_MLX5DV_ST_SZ_BYTES(query_qp_out)] = {};
    ucs_status_t status;

    status = uct_rc_gdaki_channel_query(iface, channel, out);
    if (status != UCS_OK) {
        return status;
    }

    *state_p        = UCT_IB_MLX5DV_GET(query_qp_out, out, qpc.state);
    *next_rcv_psn_p = UCT_IB_MLX5DV_GET(query_qp_out, out, qpc.next_rcv_psn);
    return UCS_OK;
}

/*
 * Status of the error of a channel whose QP failed by itself. The errors after
 * which the peer may not have received the operations, when the transport or
 * RNR retries ran out or the QP was flushed, are reported as
 * UCS_ERR_ENDPOINT_TIMEOUT. The error completion shows the cause only until
 * the flush completions of the WQEs that follow replace it in the collapsed
 * CQ, so the cause is read from the QP context extension.
 */
static ucs_status_t
uct_rc_gdaki_channel_err_status(uct_rc_gdaki_iface_t *iface,
                                uct_rc_gdaki_ep_t *ep, unsigned ch)
{
    uct_ib_mlx5_md_t *md = uct_ib_mlx5_iface_md(&iface->super.super.super);
    uct_rc_gdaki_channel_t *channel = &ep->channel_block->channels[ch];
    char out[UCT_IB_MLX5DV_ST_SZ_BYTES(query_qp_out)] = {};
    uint8_t syndrome;
    ucs_status_t status;

    if (!(md->flags & UCT_IB_MLX5_MD_FLAG_QP_ERROR_SYNDROME)) {
        ucs_diag("ep %p: channel %u qp 0x%x failed, the device does not report"
                 " the error syndrome, assuming a transport error", ep, ch,
                 channel->qp.super.qp_num);
        return UCS_ERR_ENDPOINT_TIMEOUT;
    }

    status = uct_rc_gdaki_channel_query(iface, channel, out);
    if (status != UCS_OK) {
        return status;
    }

    syndrome = UCT_IB_MLX5DV_GET(query_qp_out, out,
                                 qpc_data_extension.error_syndrome.syndrome);
    ucs_diag("ep %p: channel %u qp 0x%x failed, syndrome 0x%x vendor syndrome"
             " 0x%x hw syndrome 0x%x type %u", ep, ch, channel->qp.super.qp_num,
             syndrome,
             UCT_IB_MLX5DV_GET(query_qp_out, out,
                               qpc_data_extension.error_syndrome
                                       .vendor_error_syndrome),
             UCT_IB_MLX5DV_GET(query_qp_out, out,
                               qpc_data_extension.error_syndrome
                                       .hw_error_syndrome),
             UCT_IB_MLX5DV_GET(query_qp_out, out,
                               qpc_data_extension.error_syndrome
                                       .hw_syndrome_type));

    switch (syndrome) {
    case MLX5_CQE_SYNDROME_TRANSPORT_RETRY_EXC_ERR:
    case MLX5_CQE_SYNDROME_RNR_RETRY_EXC_ERR:
    case MLX5_CQE_SYNDROME_WR_FLUSH_ERR:
        return UCS_ERR_ENDPOINT_TIMEOUT;
    default:
        return UCS_ERR_IO_ERROR;
    }
}

/* Status of the error of a failed endpoint, from its first channel whose QP
 * failed by itself */
static ucs_status_t
uct_rc_gdaki_ep_err_status(uct_rc_gdaki_iface_t *iface, uct_rc_gdaki_ep_t *ep)
{
    unsigned ch, state;
    uint32_t next_rcv_psn;
    ucs_status_t status;

    for (ch = 0; ch < iface->num_channels; ++ch) {
        status = uct_rc_gdaki_channel_query_state(
                iface, &ep->channel_block->channels[ch], &state,
                &next_rcv_psn);
        if (status != UCS_OK) {
            return status;
        }

        if (state == UCT_RC_GDAKI_QP_STATE_ERR) {
            return uct_rc_gdaki_channel_err_status(iface, ep, ch);
        }
    }

    return UCS_ERR_IO_ERROR;
}

static ucs_status_t
uct_rc_gdaki_failover_write_dev_ep(uct_rc_gdaki_iface_t *iface,
                                   uct_rc_gdaki_ep_t *ep, size_t offset,
                                   uint8_t value)
{
    uint8_t *buf = uct_rc_gdaki_failover_scratch(iface);

    buf[0] = value;
    return uct_rc_gdaki_failover_copy(
            iface, 1, (CUdeviceptr)(ep->channel_block->gpu_ptr + offset), buf,
            1);
}

/* Report a device QP error from the progress context, as other transports
 * report errors read from their own CQs */
static unsigned uct_rc_gdaki_ep_err_progress(void *arg)
{
    uct_rc_gdaki_ep_t *ep       = arg;
    uct_rc_gdaki_iface_t *iface = ucs_derived_of(ep->super.super.iface,
                                                 uct_rc_gdaki_iface_t);
    ucs_status_t status;

    status = uct_rc_gdaki_ep_err_status(iface, ep);
    ucs_debug("ep %p: device channel QP failed: %s", ep,
              ucs_status_string(status));

    /* Failover handles only transport errors, the device reports others */
    if (ep->failover && ep->dev_ep_init &&
        (status != UCS_ERR_ENDPOINT_TIMEOUT)) {
        (void)uct_rc_gdaki_failover_write_dev_ep(
                iface, ep, ucs_offsetof(uct_rc_gdaki_dev_ep_t, error), 1);
    }

    uct_iface_handle_ep_err(ep->super.super.iface, &ep->super.super, status);
    return 1;
}

static uct_rc_gdaki_ep_t *
uct_rc_gdaki_iface_find_ep_by_cqn(uct_rc_gdaki_iface_t *iface, uint32_t cqn)
{
    uct_rc_gdaki_ep_t *ep;
    unsigned i;

    ucs_list_for_each(ep, &iface->ep_list, list) {
        for (i = 0; i < iface->num_channels; ++i) {
            if (ep->channel_block->channels[i].cq.cq_num == cqn) {
                return ep;
            }
        }
    }

    return NULL;
}

/* Called with the async lock held, so the endpoint list is stable */
static void uct_rc_gdaki_iface_err_event_handler(int fd,
                                                 ucs_event_set_types_t events,
                                                 void *arg)
{
    uct_rc_gdaki_iface_t *iface = arg;
    uct_priv_worker_t *worker   = iface->super.super.super.super.worker;
    struct mlx5dv_devx_async_event_hdr event;
    uct_rc_gdaki_ep_t *ep;
    ssize_t ret;

    for (;;) {
        ret = mlx5dv_devx_get_event(iface->err_event_channel, &event,
                                    sizeof(event));
        if (ret < 0) {
            if (errno != EAGAIN) {
                ucs_warn("%s: mlx5dv_devx_get_event() failed: %m",
                         uct_ib_device_name(uct_ib_iface_device(
                                 &iface->super.super.super)));
            }
            return;
        }

        /* The endpoint may be already destroyed, or reported */
        ep = uct_rc_gdaki_iface_find_ep_by_cqn(iface, (uint32_t)event.cookie);
        if ((ep == NULL) || ep->error_scheduled) {
            continue;
        }

        ep->error_scheduled = 1;
        ucs_callbackq_add_oneshot(&worker->super.progress_q, ep,
                                  uct_rc_gdaki_ep_err_progress, ep);
    }
}

static uint32_t uct_rc_gdaki_wqe_opcode(const void *wqe)
{
    return be32toh(*(const uint32_t*)wqe) & 0xff;
}

static uint32_t uct_rc_gdaki_wqe_be32(const void *wqe, size_t offset)
{
    uint32_t value;

    memcpy(&value, UCS_PTR_BYTE_OFFSET(wqe, offset), sizeof(value));
    return be32toh(value);
}

/*
 * Offset of the data segment byte count in the WQEs the device posts, see
 * uct_rc_mlx5_gda_wqe_prepare_put_or_atomic()
 */
#define UCT_RC_GDAKI_WQE_DATA_OFFSET 32

/* Offsets of the remote and local addresses of the device RDMA writes */
#define UCT_RC_GDAKI_WQE_RADDR_OFFSET 16
#define UCT_RC_GDAKI_WQE_LADDR_OFFSET 40

static void uct_rc_gdaki_wqe_add_be64(void *wqe, size_t offset, uint64_t add)
{
    uint64_t value;

    memcpy(&value, UCS_PTR_BYTE_OFFSET(wqe, offset), sizeof(value));
    value = htobe64(be64toh(value) + add);
    memcpy(UCS_PTR_BYTE_OFFSET(wqe, offset), &value, sizeof(value));
}

/* Number of packets a device WQE is sent in, as the device counts them */
static unsigned
uct_rc_gdaki_wqe_num_packets(const void *wqe, unsigned log_path_mtu)
{
    uint32_t length;

    switch (uct_rc_gdaki_wqe_opcode(wqe)) {
    case MLX5_OPCODE_NOP:
        return 0;
    case MLX5_OPCODE_RDMA_WRITE:
        break;
    default:
        return 1;
    }

    length = uct_rc_gdaki_wqe_be32(wqe, UCT_RC_GDAKI_WQE_DATA_OFFSET);
    return (length == 0) ? 1 :
           (length + UCS_BIT(log_path_mtu) - 1) >> log_path_mtu;
}

static uint32_t uct_rc_gdaki_iface_wqe_num(uct_rc_gdaki_iface_t *iface)
{
    return uct_ib_mlx5_devx_sq_length(iface->super.super.config.tx_qp_len);
}

/* Device address of a WQE of a channel */
static CUdeviceptr
uct_rc_gdaki_ep_wqe_addr(uct_rc_gdaki_iface_t *iface, uct_rc_gdaki_ep_t *ep,
                         unsigned ch, uint64_t wqe_idx)
{
    uint32_t wqe_num = uct_rc_gdaki_iface_wqe_num(iface);

    return (CUdeviceptr)(ep->channel_block->gpu_ptr + UCT_GDAKI_DEV_EP_SIZE +
                         (iface->num_channels * UCT_GDAKI_DEV_QP_SIZE) +
                         ((((uint64_t)ch * wqe_num) + (wqe_idx & (wqe_num - 1)))
                          << DOCA_GPUNETIO_IB_MLX5_WQE_SQ_SHIFT));
}

/*
 * Copy WQEs [first, last) of a channel between device memory and the host
 * buffer @a wqes, which holds the work queue at the same slots
 */
static ucs_status_t
uct_rc_gdaki_failover_copy_wqes(uct_rc_gdaki_iface_t *iface,
                                uct_rc_gdaki_ep_t *ep, unsigned ch,
                                int to_device, void *wqes, uint64_t first,
                                uint64_t last)
{
    uint32_t wqe_num = uct_rc_gdaki_iface_wqe_num(iface);
    uint64_t count;
    uint32_t slot;
    ucs_status_t status;

    while (first != last) {
        slot   = first & (wqe_num - 1);
        count  = ucs_min(last - first, wqe_num - slot);
        status = uct_rc_gdaki_failover_copy(
                iface, to_device, uct_rc_gdaki_ep_wqe_addr(iface, ep, ch, first),
                UCS_PTR_BYTE_OFFSET(wqes,
                                    slot << DOCA_GPUNETIO_IB_MLX5_WQE_SQ_SHIFT),
                count << DOCA_GPUNETIO_IB_MLX5_WQE_SQ_SHIFT);
        if (status != UCS_OK) {
            return status;
        }

        first += count;
    }

    return UCS_OK;
}

static ucs_status_t
uct_rc_gdaki_failover_write_paused(uct_rc_gdaki_iface_t *iface,
                                   uct_rc_gdaki_ep_t *ep, uint8_t paused)
{
    return uct_rc_gdaki_failover_write_dev_ep(
            iface, ep, ucs_offsetof(uct_rc_gdaki_dev_ep_t, paused), paused);
}

/* Write the doorbell record of a channel and ring its doorbell with the
 * control segment of the last WQE */
static ucs_status_t
uct_rc_gdaki_failover_ring_db(uct_rc_gdaki_iface_t *iface,
                              uct_rc_gdaki_ep_t *ep, unsigned ch,
                              uint16_t wqe_next, const void *last_wqe)
{
    uint32_t *buf = uct_rc_gdaki_failover_scratch(iface);
    ucs_status_t status;

    buf[0] = htobe32(wqe_next);
    status = uct_rc_gdaki_failover_copy(
            iface, 1,
            uct_rc_gdaki_ep_dev_qp_addr(ep, ch) +
                    ucs_offsetof(uct_rc_gdaki_dev_qp_t, qp_dbrec) +
                    (sizeof(uint32_t) * MLX5_SND_DBR),
            buf, sizeof(uint32_t));
    if (status != UCS_OK) {
        return status;
    }

    if (last_wqe != NULL) {
        ucs_memory_bus_store_fence();
        *(volatile uint64_t*)iface->uar_wrap->uar->super.addr.ptr =
                *(const uint64_t*)last_wqe;
        ucs_memory_bus_store_fence();
    }

    return UCS_OK;
}

/* Write the completion of a channel: of WQE wqe_cnt, or none if NULL */
static ucs_status_t
uct_rc_gdaki_failover_write_cqe(uct_rc_gdaki_iface_t *iface,
                                uct_rc_gdaki_ep_t *ep, unsigned ch,
                                const uint16_t *wqe_cnt)
{
    uint8_t *buf = uct_rc_gdaki_failover_scratch(iface);

    if (wqe_cnt == NULL) {
        /* As initialized in uct_rc_gdaki_ep_get_device_ep() */
        memset(buf, 0xff, 4);
    } else {
        buf[0] = *wqe_cnt >> 8; /* wqe_counter, big endian */
        buf[1] = *wqe_cnt & 0xff;
        buf[2] = 0;             /* signature */
        buf[3] = 0;             /* op_own: MLX5_CQE_REQ */
    }

    return uct_rc_gdaki_failover_copy(
            iface, 1,
            uct_rc_gdaki_ep_dev_qp_addr(ep, ch) +
                    ucs_offsetof(uct_rc_gdaki_dev_qp_t, cq_buff) +
                    ucs_offsetof(struct mlx5_cqe64, wqe_counter),
            buf, 4);
}

/* Move the QPs of all channels to the error state, unless they are there */
static ucs_status_t
uct_rc_gdaki_ep_stop_qps(uct_rc_gdaki_iface_t *iface, uct_rc_gdaki_ep_t *ep)
{
    uct_rc_gdaki_channel_t *channel;
    uint32_t next_rcv_psn;
    ucs_status_t status;
    unsigned ch, state;

    for (ch = 0; ch < iface->num_channels; ++ch) {
        channel = &ep->channel_block->channels[ch];
        status  = uct_rc_gdaki_channel_query_state(iface, channel, &state,
                                                   &next_rcv_psn);
        if (status != UCS_OK) {
            return status;
        }

        if (state != UCT_RC_GDAKI_QP_STATE_ERR) {
            status = uct_ib_mlx5_modify_qp_state(&iface->super.super.super,
                                                 &channel->qp.super,
                                                 IBV_QPS_ERR);
            if (status != UCS_OK) {
                return status;
            }
        }
    }

    return UCS_OK;
}

static ucs_status_t
uct_rc_gdaki_ep_failover_pause(uct_ep_h tl_ep, void *rx_token,
                               size_t *rx_token_length_p)
{
    uct_rc_gdaki_ep_t *ep       = ucs_derived_of(tl_ep, uct_rc_gdaki_ep_t);
    uct_rc_gdaki_iface_t *iface = ucs_derived_of(tl_ep->iface,
                                                 uct_rc_gdaki_iface_t);
    size_t length               = iface->num_channels * sizeof(uint32_t);
    uct_rc_gdaki_dev_qp_t *dev_qp;
    uint32_t next_rcv_psn;
    ucs_status_t status;
    unsigned ch, state;

    if (!ep->failover || (*rx_token_length_p < length)) {
        return UCS_ERR_INVALID_PARAM;
    }

    /* New device operations wait from now on */
    if (!ep->paused) {
        if (ep->dev_ep_init) {
            status = uct_rc_gdaki_failover_write_paused(iface, ep, 1);
            if (status != UCS_OK) {
                return status;
            }
        }

        ep->paused = 1;
    }

    /* A QP in error does not send or receive, so its receive PSN is final */
    status = uct_rc_gdaki_ep_stop_qps(iface, ep);
    if (status != UCS_OK) {
        return status;
    }

    /* The device must finish the WQEs it reserved, so the work queues are
     * final */
    if (ep->dev_ep_init) {
        for (ch = 0; ch < iface->num_channels; ++ch) {
            status = uct_rc_gdaki_failover_read_dev_qp(iface, ep, ch, &dev_qp);
            if (status != UCS_OK) {
                return status;
            }

            if (dev_qp->sq_rsvd_index != dev_qp->sq_ready_index) {
                return UCS_ERR_NO_RESOURCE;
            }
        }
    }

    for (ch = 0; ch < iface->num_channels; ++ch) {
        status = uct_rc_gdaki_channel_query_state(
                iface, &ep->channel_block->channels[ch], &state,
                &next_rcv_psn);
        if (status != UCS_OK) {
            return status;
        }

        ((uint32_t*)rx_token)[ch] = htobe32(next_rcv_psn);
    }

    *rx_token_length_p = length;
    return UCS_OK;
}

/*
 * Skip the first packets of an RDMA write WQE, which the peer received. A
 * packet carries a full path MTU of data, except the last one.
 */
static void uct_rc_gdaki_wqe_skip_packets(void *wqe, unsigned num_packets,
                                          unsigned log_path_mtu)
{
    uint32_t offset = num_packets << log_path_mtu;
    uint32_t length = uct_rc_gdaki_wqe_be32(wqe, UCT_RC_GDAKI_WQE_DATA_OFFSET);

    ucs_assert(uct_rc_gdaki_wqe_opcode(wqe) == MLX5_OPCODE_RDMA_WRITE);
    ucs_assert(offset < length);

    uct_rc_gdaki_wqe_add_be64(wqe, UCT_RC_GDAKI_WQE_RADDR_OFFSET, offset);
    uct_rc_gdaki_wqe_add_be64(wqe, UCT_RC_GDAKI_WQE_LADDR_OFFSET, offset);
    length = htobe32(length - offset);
    memcpy(UCS_PTR_BYTE_OFFSET(wqe, UCT_RC_GDAKI_WQE_DATA_OFFSET), &length,
           sizeof(length));
}

/*
 * Find the WQEs of a channel the peer did not receive, from the first one it
 * did not receive in full to the last ready one, and copy the work queue to
 * @a wq. The device counts the packets of the ready WQEs, which is the PSN
 * that follows them, so the first WQE is found by walking back from the last
 * one. The copy of the first WQE skips the packets the peer received.
 */
static ucs_status_t
uct_rc_gdaki_ep_find_lost(uct_rc_gdaki_iface_t *iface, uct_rc_gdaki_ep_t *ep,
                          unsigned ch, uint32_t peer_rcv_psn, void *wq,
                          uint64_t *first_p, uint64_t *ready_p,
                          uint32_t *ready_psn_p, uint32_t *num_packets_p)
{
    uct_rc_gdaki_channel_t *channel = &ep->channel_block->channels[ch];
    uint32_t wqe_num                = uct_rc_gdaki_iface_wqe_num(iface);
    void *wqe                       = NULL;
    uct_rc_gdaki_dev_qp_t *dev_qp;
    uint32_t ready_psn, end_psn, num_lost, num_packets;
    uint64_t ready, first;
    ucs_status_t status;

    status = uct_rc_gdaki_failover_read_dev_qp(iface, ep, ch, &dev_qp);
    if (status != UCS_OK) {
        return status;
    }

    ready     = dev_qp->sq_ready_index;
    ready_psn = dev_qp->sq_ready_psn;
    end_psn   = ready_psn & UCT_IB_MLX5_PSN_MASK;
    num_lost  = (end_psn - peer_rcv_psn) & UCT_IB_MLX5_PSN_MASK;

    status = uct_rc_gdaki_failover_copy_wqes(
            iface, ep, ch, 0, wq, (ready > wqe_num) ? (ready - wqe_num) : 0,
            ready);
    if (status != UCS_OK) {
        return status;
    }

    num_packets = 0;
    first       = ready;
    while (num_packets < num_lost) {
        if ((first == 0) || ((ready - first) >= wqe_num)) {
            ucs_error("ep %p: channel %u: %u lost packets are not in the work"
                      " queue, end psn 0x%x peer psn 0x%x",
                      ep, ch, num_lost, end_psn, peer_rcv_psn);
            return UCS_ERR_IO_ERROR;
        }

        --first;
        wqe          = UCS_PTR_BYTE_OFFSET(
                wq, (first & (wqe_num - 1)) << DOCA_GPUNETIO_IB_MLX5_WQE_SQ_SHIFT);
        num_packets += uct_rc_gdaki_wqe_num_packets(wqe, ep->log_path_mtu);
    }

    ucs_debug("ep %p: channel %u qp 0x%x: ready %" PRIu64 " first %" PRIu64
              " end psn 0x%x peer psn 0x%x lost packets %u, received %u"
              " packets of the first WQE", ep, ch, channel->qp.super.qp_num,
              ready, first, end_psn, peer_rcv_psn, num_lost,
              num_packets - num_lost);

    if (num_packets > num_lost) {
        uct_rc_gdaki_wqe_skip_packets(wqe, num_packets - num_lost,
                                      ep->log_path_mtu);
    }

    *first_p       = first;
    *ready_p       = ready;
    *ready_psn_p   = ready_psn;
    *num_packets_p = num_lost;
    return UCS_OK;
}

/*
 * Offsets in the WQEs the device posts, see
 * uct_rc_mlx5_gda_wqe_prepare_put_or_atomic(): control, remote address, then
 * the atomic segment for atomics, then the data segment.
 */
#define UCT_RC_GDAKI_WQE_RKEY_OFFSET          24
#define UCT_RC_GDAKI_WQE_PUT_LKEY_OFFSET      36
#define UCT_RC_GDAKI_WQE_ATOMIC_LKEY_OFFSET   52
#define UCT_RC_GDAKI_WQE_ATOMIC_LADDR_OFFSET  56

static void uct_rc_gdaki_wqe_set32(void *wqe, size_t offset, uint32_t value)
{
    memcpy(UCS_PTR_BYTE_OFFSET(wqe, offset), &value, sizeof(value));
}

static uint32_t uct_rc_gdaki_wqe_get32(const void *wqe, size_t offset)
{
    uint32_t value;

    memcpy(&value, UCS_PTR_BYTE_OFFSET(wqe, offset), sizeof(value));
    return value;
}

/*
 * Rewrite a WQE of a channel of ep for a channel of to_ep: WQE index, QP
 * number, and the keys of the other device. An atomic writes its result to
 * the atomic buffer of the interface.
 */
static ucs_status_t
uct_rc_gdaki_failover_rewrite_wqe(uct_rc_gdaki_iface_t *to_iface,
                                  uint32_t to_qp_num, void *wqe,
                                  uint16_t wqe_idx,
                                  uct_ep_failover_key_cb_t key_cb, void *arg)
{
    struct mlx5_wqe_ctrl_seg *ctrl = wqe;
    uint32_t opcode                = uct_rc_gdaki_wqe_opcode(wqe);
    uint32_t ds                    = be32toh(ctrl->qpn_ds) & 0x3f;
    uint32_t key;
    uint64_t laddr;
    ucs_status_t status;

    ctrl->opmod_idx_opcode = htobe32(((uint32_t)wqe_idx << 8) | opcode);
    ctrl->qpn_ds           = htobe32((to_qp_num << 8) | ds);
    if (opcode == MLX5_OPCODE_NOP) {
        return UCS_OK; /* Posted by a device flush */
    }

    status = key_cb(arg, 1,
                    uct_rc_gdaki_wqe_get32(wqe, UCT_RC_GDAKI_WQE_RKEY_OFFSET),
                    &key);
    if (status != UCS_OK) {
        return status;
    }

    uct_rc_gdaki_wqe_set32(wqe, UCT_RC_GDAKI_WQE_RKEY_OFFSET, key);

    switch (opcode) {
    case MLX5_OPCODE_RDMA_WRITE:
        status = key_cb(arg, 0,
                        uct_rc_gdaki_wqe_get32(wqe,
                                               UCT_RC_GDAKI_WQE_PUT_LKEY_OFFSET),
                        &key);
        if (status != UCS_OK) {
            return status;
        }

        uct_rc_gdaki_wqe_set32(wqe, UCT_RC_GDAKI_WQE_PUT_LKEY_OFFSET, key);
        return UCS_OK;
    case MLX5_OPCODE_ATOMIC_FA:
        uct_rc_gdaki_wqe_set32(wqe, UCT_RC_GDAKI_WQE_ATOMIC_LKEY_OFFSET,
                               htonl(to_iface->atomic_mr->lkey));
        laddr = htobe64((uintptr_t)to_iface->atomic_buff);
        memcpy(UCS_PTR_BYTE_OFFSET(wqe, UCT_RC_GDAKI_WQE_ATOMIC_LADDR_OFFSET),
               &laddr, sizeof(laddr));
        return UCS_OK;
    default:
        ucs_error("device WQE opcode 0x%x cannot be moved", opcode);
        return UCS_ERR_UNSUPPORTED;
    }
}

/* WQE indexes of a channel: reserved, ready, and the last completed one as
 * the device computes it */
static ucs_status_t
uct_rc_gdaki_failover_indexes(uct_rc_gdaki_iface_t *iface,
                              uct_rc_gdaki_ep_t *ep, unsigned ch,
                              uint64_t *rsvd_p, uint64_t *ready_p,
                              uint64_t *completed_p, uint32_t *ready_psn_p)
{
    uct_rc_gdaki_dev_qp_t *dev_qp;
    struct mlx5_cqe64 *cqe;
    ucs_status_t status;
    uint16_t wqe_cnt;

    status = uct_rc_gdaki_failover_read_dev_qp(iface, ep, ch, &dev_qp);
    if (status != UCS_OK) {
        return status;
    }

    cqe = (struct mlx5_cqe64*)dev_qp->cq_buff;
    if ((cqe->op_own >> 4) == MLX5_CQE_REQ_ERR) {
        return UCS_ERR_IO_ERROR;
    }

    wqe_cnt      = be16toh(cqe->wqe_counter);
    *rsvd_p      = dev_qp->sq_rsvd_index;
    *ready_p     = dev_qp->sq_ready_index;
    *ready_psn_p = dev_qp->sq_ready_psn;
    *completed_p = *rsvd_p - ((*rsvd_p - wqe_cnt) & 0xffff);
    return UCS_OK;
}

/* Post the lost WQEs of a channel of ep to the same channel of to_ep, whose
 * device posting is paused */
static ucs_status_t
uct_rc_gdaki_failover_move_channel(uct_rc_gdaki_iface_t *iface,
                                   uct_rc_gdaki_ep_t *ep,
                                   uct_rc_gdaki_iface_t *to_iface,
                                   uct_rc_gdaki_ep_t *to_ep, unsigned ch,
                                   uint32_t peer_rcv_psn,
                                   uct_ep_failover_key_cb_t key_cb, void *arg)
{
    uct_rc_gdaki_channel_t *channel = &ep->channel_block->channels[ch];
    uint32_t wqe_num                = uct_rc_gdaki_iface_wqe_num(iface);
    uint32_t to_qp_num = to_ep->channel_block->channels[ch].qp.super.qp_num;
    void *wq           = iface->failover.buf;
    void *to_wq        = UCS_PTR_BYTE_OFFSET(
            to_iface->failover.buf,
            (size_t)wqe_num << DOCA_GPUNETIO_IB_MLX5_WQE_SQ_SHIFT);
    uint32_t ready_psn, num_packets, to_ready_psn;
    uint64_t first, ready, idx, to_rsvd, to_completed, to_ready;
    uint64_t *indexes;
    void *wqe;
    ucs_status_t status;

    status = uct_rc_gdaki_ep_find_lost(iface, ep, ch, peer_rcv_psn, wq, &first,
                                       &ready, &ready_psn, &num_packets);
    if (status != UCS_OK) {
        return status;
    }

    channel->moved_ready = ready;
    channel->moved_end   = 0;
    if (first == ready) {
        return UCS_OK;
    }

    /* Room in the work queue of to_ep, as the device computes it */
    status = uct_rc_gdaki_failover_indexes(to_iface, to_ep, ch, &to_rsvd,
                                           &to_ready, &to_completed,
                                           &to_ready_psn);
    if (status != UCS_OK) {
        return status;
    }

    ucs_assert(to_ready == to_rsvd);
    if ((to_rsvd + (ready - first)) > (to_completed + wqe_num + 1)) {
        return UCS_ERR_NO_RESOURCE;
    }

    for (idx = first; idx < ready; ++idx) {
        wqe = UCS_PTR_BYTE_OFFSET(to_wq, ((to_ready + idx - first) &
                                          (wqe_num - 1))
                                                 << DOCA_GPUNETIO_IB_MLX5_WQE_SQ_SHIFT);
        memcpy(wqe,
               UCS_PTR_BYTE_OFFSET(wq, (idx & (wqe_num - 1))
                                               << DOCA_GPUNETIO_IB_MLX5_WQE_SQ_SHIFT),
               UCS_BIT(DOCA_GPUNETIO_IB_MLX5_WQE_SQ_SHIFT));
        status = uct_rc_gdaki_failover_rewrite_wqe(to_iface, to_qp_num, wqe,
                                                   to_ready + idx - first,
                                                   key_cb, arg);
        if (status != UCS_OK) {
            return status;
        }
    }

    /* The device requests completions only for some WQEs, and the host waits
     * for the completion of the last moved one */
    wqe = UCS_PTR_BYTE_OFFSET(to_wq, ((to_ready + (ready - first) - 1) &
                                      (wqe_num - 1))
                                             << DOCA_GPUNETIO_IB_MLX5_WQE_SQ_SHIFT);
    ((struct mlx5_wqe_ctrl_seg*)wqe)->fm_ce_se |= MLX5_WQE_CTRL_CQ_UPDATE;

    status = uct_rc_gdaki_failover_copy_wqes(to_iface, to_ep, ch, 1, to_wq,
                                             to_ready,
                                             to_ready + (ready - first));
    if (status != UCS_OK) {
        return status;
    }

    /* The device continues after the moved WQEs */
    indexes    = uct_rc_gdaki_failover_scratch(to_iface);
    indexes[0] = to_ready + (ready - first); /* sq_rsvd_index */
    indexes[1] = indexes[0];                 /* sq_ready_index */
    indexes[2] = to_ready_psn + num_packets;
    UCS_STATIC_ASSERT(ucs_offsetof(uct_rc_gdaki_dev_qp_t, sq_ready_index) ==
                      ucs_offsetof(uct_rc_gdaki_dev_qp_t, sq_rsvd_index) +
                      sizeof(uint64_t));
    status = uct_rc_gdaki_failover_copy(
            to_iface, 1,
            uct_rc_gdaki_ep_dev_qp_addr(to_ep, ch) +
                    ucs_offsetof(uct_rc_gdaki_dev_qp_t, sq_rsvd_index),
            indexes, 2 * sizeof(uint64_t));
    if (status != UCS_OK) {
        return status;
    }

    *(uint32_t*)indexes = (uint32_t)indexes[2];
    status = uct_rc_gdaki_failover_copy(
            to_iface, 1,
            uct_rc_gdaki_ep_dev_qp_addr(to_ep, ch) +
                    ucs_offsetof(uct_rc_gdaki_dev_qp_t, sq_ready_psn),
            indexes, sizeof(uint32_t));
    if (status != UCS_OK) {
        return status;
    }

    ucs_debug("ep %p: channel %u: moved WQEs %" PRIu64 "..%" PRIu64
              " to ep %p WQEs %" PRIu64 "..%" PRIu64,
              ep, ch, first, ready, to_ep, to_ready,
              to_ready + (ready - first));
    channel->moved_end = to_ready + (ready - first);
    return uct_rc_gdaki_failover_ring_db(
            to_iface, to_ep, ch, channel->moved_end,
            UCS_PTR_BYTE_OFFSET(to_wq, ((channel->moved_end - 1) &
                                        (wqe_num - 1))
                                               << DOCA_GPUNETIO_IB_MLX5_WQE_SQ_SHIFT));
}

static ucs_status_t
uct_rc_gdaki_ep_failover_move(uct_ep_h tl_ep, uct_ep_h tl_to_ep,
                              const void *peer_rx_token,
                              size_t peer_rx_token_length,
                              uct_ep_failover_key_cb_t key_cb, void *arg)
{
    uct_rc_gdaki_ep_t *ep          = ucs_derived_of(tl_ep, uct_rc_gdaki_ep_t);
    uct_rc_gdaki_iface_t *iface    = ucs_derived_of(tl_ep->iface,
                                                    uct_rc_gdaki_iface_t);
    uct_rc_gdaki_ep_t *to_ep       = ucs_derived_of(tl_to_ep,
                                                    uct_rc_gdaki_ep_t);
    uct_rc_gdaki_iface_t *to_iface = ucs_derived_of(tl_to_ep->iface,
                                                    uct_rc_gdaki_iface_t);
    uint64_t to_rsvd, to_ready, to_completed;
    uint32_t to_ready_psn;
    uint16_t wqe_cnt;
    ucs_status_t status;
    uint8_t *buf;
    unsigned ch;

    if (!ep->failover || !ep->paused ||
        (ucs_derived_of(tl_to_ep->iface, uct_base_iface_t)->internal_ops !=
         ucs_derived_of(tl_ep->iface, uct_base_iface_t)->internal_ops) ||
        (to_iface->num_channels != iface->num_channels) ||
        (peer_rx_token_length != (iface->num_channels * sizeof(uint32_t)))) {
        return UCS_ERR_INVALID_PARAM;
    }

    if (!ep->dev_ep_init) {
        /* No memory list refers to the device endpoint */
        return UCS_OK;
    }

    if (!to_ep->dev_ep_init || to_ep->paused) {
        return UCS_ERR_UNSUPPORTED;
    }

    if (ep->move_state == UCT_RC_GDAKI_MOVE_STATE_NONE) {
        /* Stop the device from posting to to_ep meanwhile */
        status = uct_rc_gdaki_failover_write_paused(to_iface, to_ep, 1);
        if (status != UCS_OK) {
            return status;
        }

        for (ch = 0; ch < iface->num_channels; ++ch) {
            ep->channel_block->channels[ch].moved_ready = 0;
            ep->channel_block->channels[ch].moved_end   = 0;
        }

        ep->move_state = UCT_RC_GDAKI_MOVE_STATE_PAUSING;
    }

    if (ep->move_state == UCT_RC_GDAKI_MOVE_STATE_PAUSING) {
        /* The device must finish the WQEs it reserved on to_ep */
        for (ch = 0; ch < iface->num_channels; ++ch) {
            status = uct_rc_gdaki_failover_indexes(to_iface, to_ep, ch,
                                                   &to_rsvd, &to_ready,
                                                   &to_completed,
                                                   &to_ready_psn);
            if (status != UCS_OK) {
                goto out_unpause;
            }

            if (to_ready != to_rsvd) {
                return UCS_ERR_NO_RESOURCE;
            }
        }

        for (ch = 0; ch < iface->num_channels; ++ch) {
            if (ep->channel_block->channels[ch].moved_end != 0) {
                continue; /* Moved in a previous call */
            }

            status = uct_rc_gdaki_failover_move_channel(
                    iface, ep, to_iface, to_ep, ch,
                    be32toh(((const uint32_t*)peer_rx_token)[ch]), key_cb,
                    arg);
            if (status == UCS_ERR_NO_RESOURCE) {
                return status; /* to_ep is still paused */
            } else if (status != UCS_OK) {
                goto out_unpause;
            }

            if (ep->channel_block->channels[ch].moved_end == 0) {
                /* Nothing to move, mark the channel done */
                ep->channel_block->channels[ch].moved_end = UINT64_MAX;
            }
        }

        status = uct_rc_gdaki_failover_write_paused(to_iface, to_ep, 0);
        if (status != UCS_OK) {
            return status;
        }

        ep->move_state = UCT_RC_GDAKI_MOVE_STATE_POSTED;
    }

    /* The moved WQEs complete on to_ep */
    for (ch = 0; ch < iface->num_channels; ++ch) {
        if (ep->channel_block->channels[ch].moved_end == UINT64_MAX) {
            continue;
        }

        status = uct_rc_gdaki_failover_indexes(to_iface, to_ep, ch, &to_rsvd,
                                               &to_ready, &to_completed,
                                               &to_ready_psn);
        if (status != UCS_OK) {
            return status;
        }

        if ((to_completed + 1) < ep->channel_block->channels[ch].moved_end) {
            return UCS_ERR_NO_RESOURCE;
        }
    }

    /* New device operations must see the endpoint failed before the waiting
     * ones complete, see ucp_device_select_lane() */
    buf    = uct_rc_gdaki_failover_scratch(iface);
    buf[0] = UCT_DEVICE_EP_FLAG_FAILED;
    status = uct_rc_gdaki_failover_copy(
            iface, 1,
            (CUdeviceptr)(ep->channel_block->gpu_ptr +
                          ucs_offsetof(uct_device_ep_t, flags)),
            buf, 1);
    if (status != UCS_OK) {
        return status;
    }

    for (ch = 0; ch < iface->num_channels; ++ch) {
        if (ep->channel_block->channels[ch].moved_ready == 0) {
            continue;
        }

        wqe_cnt = ep->channel_block->channels[ch].moved_ready - 1;
        status  = uct_rc_gdaki_failover_write_cqe(iface, ep, ch, &wqe_cnt);
        if (status != UCS_OK) {
            return status;
        }
    }

    ep->move_state = UCT_RC_GDAKI_MOVE_STATE_DONE;
    return UCS_OK;

out_unpause:
    (void)uct_rc_gdaki_failover_write_paused(to_iface, to_ep, 0);
    ep->move_state = UCT_RC_GDAKI_MOVE_STATE_NONE;
    return status;
}

static ucs_status_t uct_rc_gdaki_ep_failover_fail(uct_ep_h tl_ep)
{
    uct_rc_gdaki_ep_t *ep       = ucs_derived_of(tl_ep, uct_rc_gdaki_ep_t);
    uct_rc_gdaki_iface_t *iface = ucs_derived_of(tl_ep->iface,
                                                 uct_rc_gdaki_iface_t);
    ucs_status_t status;

    if (!ep->failover) {
        return UCS_ERR_INVALID_PARAM;
    }

    /* Operations on the channels that did not fail fail as well */
    status = uct_rc_gdaki_ep_stop_qps(iface, ep);
    if (status != UCS_OK) {
        return status;
    }

    if (ep->dev_ep_init) {
        status = uct_rc_gdaki_failover_write_dev_ep(
                iface, ep, ucs_offsetof(uct_rc_gdaki_dev_ep_t, error), 1);
        if (status != UCS_OK) {
            return status;
        }

        if (ep->paused) {
            status = uct_rc_gdaki_failover_write_paused(iface, ep, 0);
            if (status != UCS_OK) {
                return status;
            }
        }
    }

    ucs_debug("ep %p: failed, device operations fail", ep);
    ep->paused = 0;
    return UCS_OK;
}

static UCS_CLASS_INIT_FUNC(uct_rc_gdaki_ep_t, const uct_ep_params_t *params)
{
    uct_rc_gdaki_iface_t *iface = ucs_derived_of(params->iface,
                                                 uct_rc_gdaki_iface_t);
    ucs_status_t status;

    status = uct_rc_gdaki_iface_cuda_ctx_init(iface);
    if (status != UCS_OK) {
        return status;
    }

    UCS_CLASS_CALL_SUPER_INIT(uct_base_ep_t, &iface->super.super.super.super);

    self->dev_ep_init     = 0;
    self->error_scheduled = 0;
    self->failover        = (params->field_mask & UCT_EP_PARAM_FIELD_FLAGS) &&
                            (params->flags & UCT_EP_PARAM_FLAG_FAILOVER);
    self->log_path_mtu    = ucs_ilog2(uct_ib_mtu_value(IBV_MTU_4096));
    self->paused          = 0;
    self->move_state      = UCT_RC_GDAKI_MOVE_STATE_NONE;
    uct_rc_gdaki_ep_reset_channels(self);

    if (self->failover) {
        status = uct_rc_gdaki_iface_failover_init(iface);
        if (status != UCS_OK) {
            return status;
        }
    }

    status = uct_rc_gdaki_ep_init_channels(iface, self);
    if (status != UCS_OK) {
        return status;
    }

    UCS_ASYNC_BLOCK(iface->super.super.super.super.worker->async);
    ucs_list_add_tail(&iface->ep_list, &self->list);
    UCS_ASYNC_UNBLOCK(iface->super.super.super.super.worker->async);
    return UCS_OK;
}

static int
uct_rc_gdaki_ep_err_progress_filter(const ucs_callbackq_elem_t *elem,
                                    void *arg)
{
    return (elem->cb == uct_rc_gdaki_ep_err_progress) && (elem->arg == arg);
}

static UCS_CLASS_CLEANUP_FUNC(uct_rc_gdaki_ep_t)
{
    uct_rc_gdaki_iface_t *iface = ucs_derived_of(self->super.super.iface,
                                                 uct_rc_gdaki_iface_t);
    uct_priv_worker_t *worker   = iface->super.super.super.super.worker;

    /* No error event can reach this endpoint anymore */
    UCS_ASYNC_BLOCK(worker->async);
    ucs_list_del(&self->list);
    ucs_callbackq_remove_oneshot(&worker->super.progress_q, self,
                                 uct_rc_gdaki_ep_err_progress_filter, self);
    UCS_ASYNC_UNBLOCK(worker->async);

    uct_rc_gdaki_ep_cleanup_channels(iface, self);
}

UCS_CLASS_DEFINE(uct_rc_gdaki_ep_t, uct_base_ep_t);
UCS_CLASS_DEFINE_NEW_FUNC(uct_rc_gdaki_ep_t, uct_ep_t, const uct_ep_params_t *);
UCS_CLASS_DEFINE_DELETE_FUNC(uct_rc_gdaki_ep_t, uct_ep_t);

static ucs_status_t
uct_rc_gdaki_ep_get_address(uct_ep_h tl_ep, uct_ep_addr_t *addr)
{
    uct_rc_gdaki_ep_t *ep = ucs_derived_of(tl_ep, uct_rc_gdaki_ep_t);
    uct_rc_gdaki_iface_t *iface = ucs_derived_of(tl_ep->iface,
                                                 uct_rc_gdaki_iface_t);
    void *rc_addr               = (void*)addr;
    unsigned i;

    for (i = 0; i < iface->num_channels; i++) {
        uct_ib_pack_uint24(*ucs_serialize_next(&rc_addr, uct_ib_uint24_t),
                           ep->channel_block->channels[i].qp.super.qp_num);
    }
    return UCS_OK;
}

static ucs_status_t uct_rc_gdaki_iface_get_address(uct_iface_h tl_iface,
                                                   uct_iface_addr_t *addr)
{
    *(uint8_t*)addr = UCT_RC_MLX5_IFACE_ADDR_TYPE_BASIC;
    return UCS_OK;
}

static ucs_status_t
uct_rc_gdaki_ep_connect_to_ep_v2(uct_ep_h tl_ep,
                                 const uct_device_addr_t *device_addr,
                                 const uct_ep_addr_t *ep_addr,
                                 const uct_ep_connect_to_ep_params_t *params)
{
    uct_rc_gdaki_ep_t *ep       = ucs_derived_of(tl_ep, uct_rc_gdaki_ep_t);
    uct_rc_gdaki_iface_t *iface = ucs_derived_of(tl_ep->iface,
                                                 uct_rc_gdaki_iface_t);
    uct_rc_gdaki_channel_t *channels = ep->channel_block->channels;
    const uct_ib_address_t *ib_addr  = (void*)device_addr;
    uint8_t path_index               = 0;
    struct ibv_ah_attr ah_attr;
    enum ibv_mtu path_mtu;
    uint32_t dest_qp_num;
    ucs_status_t status;
    unsigned i;

    status = uct_ib_iface_fill_ah_attr_from_addr(&iface->super.super.super,
                                                 ib_addr, path_index, &ah_attr,
                                                 &path_mtu);
    if (status != UCS_OK) {
        return status;
    }

    ucs_assert(path_mtu != UCT_IB_ADDRESS_INVALID_PATH_MTU);
    ep->log_path_mtu = ucs_ilog2(uct_ib_mtu_value(path_mtu));

    for (i = 0; i < iface->num_channels; i++) {
        dest_qp_num = uct_ib_unpack_uint24(
                *ucs_serialize_next(&ep_addr, uct_ib_uint24_t));
        status      = uct_rc_mlx5_iface_common_devx_connect_qp(
                &iface->super, &channels[i].qp.super, dest_qp_num, &ah_attr,
                path_mtu, path_index, iface->super.super.config.max_rd_atomic);
        if (status != UCS_OK) {
            return status;
        }
    }

    if (iface->err_event_channel != NULL) {
        for (i = 0; i < iface->num_channels; i++) {
            uct_rc_gdaki_cq_arm_solicited(&channels[i].cq);
        }
    }

    return UCS_OK;
}

int uct_rc_gdaki_ep_is_connected(uct_ep_h tl_ep,
                                 const uct_ep_is_connected_params_t *params)
{
    uct_rc_gdaki_ep_t *ep       = ucs_derived_of(tl_ep, uct_rc_gdaki_ep_t);
    uct_rc_gdaki_iface_t *iface = ucs_derived_of(ep->super.super.iface,
                                                 uct_rc_gdaki_iface_t);
    uint32_t addr_qp            = 0;
    uct_rc_mlx5_base_ep_address_t *rc_addr;
    ucs_status_t status;
    struct ibv_ah_attr ah_attr;
    uint32_t qp_num;
    union ibv_gid *rgid;
    const uct_ib_address_t *ib_addr;

    UCT_EP_IS_CONNECTED_CHECK_DEV_ADDR(params);

    status = uct_ib_mlx5_query_qp_peer_info(
            &iface->super.super.super, &ep->channel_block->channels[0].qp.super,
            &ah_attr, &qp_num);
    if (status != UCS_OK) {
        return 0;
    }

    /* TODO unite code with uct_rc_mlx5_base_ep_is_connected */
    if (params->field_mask & UCT_EP_IS_CONNECTED_FIELD_EP_ADDR) {
        rc_addr = (uct_rc_mlx5_base_ep_address_t*)params->ep_addr;
        addr_qp = uct_ib_unpack_uint24(rc_addr->qp_num);
    }

    if ((addr_qp != 0) && (qp_num != addr_qp)) {
        return 0;
    }

    rgid    = (ah_attr.is_global) ? &ah_attr.grh.dgid : NULL;
    ib_addr = (const uct_ib_address_t*)params->device_addr;
    return uct_ib_iface_is_same_device(ib_addr, ah_attr.dlid, rgid);
}

static ucs_status_t
uct_rc_gdaki_iface_query(uct_iface_h tl_iface, uct_iface_attr_t *iface_attr)
{
    uct_rc_gdaki_iface_t *iface = ucs_derived_of(tl_iface,
                                                 uct_rc_gdaki_iface_t);
    ucs_status_t status;

    status = uct_ib_iface_query(&iface->super.super.super, iface_attr);
    if (status != UCS_OK) {
        return status;
    }

    /* TODO:
     *  - add UCT_IFACE_FLAG_PUT_BATCH
     *  - PENDING and PUT_ZCOPY will be needed to establish rma_bw lanes
     *  - As this lane does not really support PUT_ZCOPY and PENDING, this could be
     *    causing issue when trying to send standard PUT. Eventually we must probably
     *    introduce another type of lane (rma_batch#x).
     */
    iface_attr->cap.flags = UCT_IFACE_FLAG_CONNECT_TO_EP |
                            UCT_IFACE_FLAG_INTER_NODE |
                            UCT_IFACE_FLAG_DEVICE_EP |
                            UCT_IFACE_FLAG_DEVICE_LKEY |
                            UCT_IFACE_FLAG_ERRHANDLE_PEER_FAILURE;

    iface_attr->ep_addr_len    = sizeof(uct_ib_uint24_t) * iface->num_channels;
    iface_attr->iface_addr_len = sizeof(uint8_t);
    iface_attr->overhead       = UCT_RC_MLX5_IFACE_OVERHEAD;

    iface_attr->cap.put.min_zcopy = 0;
    iface_attr->cap.put.max_zcopy =
            uct_ib_iface_port_attr(&iface->super.super.super)->max_msg_sz;
    iface_attr->ctl_device    = uct_cuda_get_sys_dev(iface->cuda.dev);
    iface_attr->dev_num_paths = 1;

    return UCS_OK;
}

ucs_status_t
uct_rc_gdaki_create_cq(uct_ib_iface_t *ib_iface, uct_ib_dir_t dir,
                       const uct_ib_iface_init_attr_t *init_attr,
                       int preferred_cpu, size_t inl)
{
    uct_rc_gdaki_iface_t *iface = ucs_derived_of(ib_iface,
                                                 uct_rc_gdaki_iface_t);

    iface->super.cq[dir].type = UCT_IB_MLX5_OBJ_TYPE_NULL;
    return UCS_OK;
}

ucs_status_t
uct_rc_gdaki_ep_get_device_ep(uct_ep_h tl_ep, uct_device_ep_h *device_ep_p)
{
    uct_rc_gdaki_ep_t *ep       = ucs_derived_of(tl_ep, uct_rc_gdaki_ep_t);
    uct_rc_gdaki_iface_t *iface = ucs_derived_of(ep->super.super.iface,
                                                 uct_rc_gdaki_iface_t);
    uct_rc_gdaki_dev_ep_t *dev_ep;
    uct_rc_gdaki_channel_t *channel;
    ucs_status_t status;
    CUdeviceptr sq_db;
    unsigned i;
    size_t dev_ep_host_size;

    pthread_mutex_lock(&iface->ep_init_lock);

    if (!ep->dev_ep_init) {
        status = UCT_CUDADRV_FUNC_LOG_ERR(cuCtxPushCurrent(iface->cuda.ctx));
        if (status != UCS_OK) {
            goto out_unlock;
        }

        dev_ep_host_size = UCT_GDAKI_DEV_EP_SIZE +
                           iface->num_channels * UCT_GDAKI_DEV_QP_SIZE;
        dev_ep           = ucs_calloc(1, dev_ep_host_size, "dev_ep");
        if (dev_ep == NULL) {
            ucs_error("failed to allocate dev_ep on host buffer size=%zu ep=%p "
                      "iface=%p",
                      dev_ep_host_size, ep, iface);
            status = UCS_ERR_NO_MEMORY;
            goto out_ctx;
        }

        dev_ep->atomic_va    = iface->atomic_buff;
        dev_ep->atomic_lkey  = htonl(iface->atomic_mr->lkey);
        dev_ep->sq_wqe_num   = uct_ib_mlx5_devx_sq_length(
                iface->super.super.config.tx_qp_len);
        dev_ep->sq_fc_mask   = (dev_ep->sq_wqe_num >> 1) - 1;
        dev_ep->channel_mask = iface->num_channels - 1;
        dev_ep->failover     = ep->failover;
        dev_ep->log_path_mtu = ep->log_path_mtu;
        dev_ep->sq_wqe_daddr = (uint8_t*)(ep->channel_block->gpu_ptr +
                                          UCT_GDAKI_DEV_EP_SIZE +
                                          iface->num_channels *
                                                  UCT_GDAKI_DEV_QP_SIZE);

        sq_db = iface->uar_wrap->device_ptr;
        for (i = 0; i < iface->num_channels; ++i) {
            channel = &ep->channel_block->channels[i];
            ucs_assert(channel->qp.reg == &iface->uar_wrap->uar->super);
            dev_ep->qps[i].sq_db  = (uint64_t*)sq_db;
            dev_ep->qps[i].sq_num = channel->qp.super.qp_num;
            dev_ep->qps[i].qpn_ds = htonl(channel->qp.super.qp_num <<
                                          DOCA_GPUNETIO_VERBS_WQE_IDX_SHIFT);
            memset(&dev_ep->qps[i].cq_buff, 0xff, 64);
        }

        status = UCT_CUDADRV_FUNC_LOG_ERR(
                cuMemcpyHtoD((CUdeviceptr)ep->channel_block->gpu_ptr, dev_ep,
                             dev_ep_host_size));
        if (status != UCS_OK) {
            goto out_free;
        }

        ucs_free(dev_ep);
        (void)UCT_CUDADRV_FUNC_LOG_WARN(cuCtxPopCurrent(NULL));

        ep->dev_ep_init = 1;
    }

    *device_ep_p = &((uct_rc_gdaki_dev_ep_t*)ep->channel_block->gpu_ptr)->super;
    pthread_mutex_unlock(&iface->ep_init_lock);
    return UCS_OK;

out_free:
    ucs_free(dev_ep);
out_ctx:
    (void)UCT_CUDADRV_FUNC_LOG_WARN(cuCtxPopCurrent(NULL));
out_unlock:
    pthread_mutex_unlock(&iface->ep_init_lock);
    return status;
}

static UCS_CLASS_DECLARE_NEW_FUNC(uct_rc_gdaki_iface_t, uct_iface_t, uct_md_h,
                                  uct_worker_h, const uct_iface_params_t*,
                                  const uct_iface_config_t*);

static UCS_CLASS_DECLARE_DELETE_FUNC(uct_rc_gdaki_iface_t, uct_iface_t);

static uct_rc_iface_ops_t uct_rc_gdaki_internal_ops = {
    .super = {
        .super = {
            .iface_query_v2         = uct_iface_base_query_v2,
            .iface_estimate_perf    = uct_ib_iface_estimate_perf,
            .iface_vfs_refresh      = (uct_iface_vfs_refresh_func_t)ucs_empty_function,
            .ep_query               = (uct_ep_query_func_t)ucs_empty_function_return_unsupported,
            .ep_invalidate          = (uct_ep_invalidate_func_t)ucs_empty_function_return_unsupported,
            .ep_connect_to_ep_v2    = uct_rc_gdaki_ep_connect_to_ep_v2,
            .iface_is_reachable_v2  = uct_ib_iface_is_reachable_v2,
            .ep_is_connected        = uct_rc_gdaki_ep_is_connected,
            .ep_get_device_ep       = uct_rc_gdaki_ep_get_device_ep,
            .ep_outstanding_purge   = (uct_ep_outstanding_purge_func_t)ucs_empty_function_return_unsupported,
            .ep_failover_pause      = uct_rc_gdaki_ep_failover_pause,
            .ep_failover_move       = uct_rc_gdaki_ep_failover_move,
            .ep_failover_fail       = uct_rc_gdaki_ep_failover_fail
        },
        .create_cq  = uct_rc_gdaki_create_cq,
        .destroy_cq = (uct_ib_iface_destroy_cq_func_t)ucs_empty_function_return_success,
    },
    .init_rx    = (uct_rc_iface_init_rx_func_t)ucs_empty_function_return_success,
    .cleanup_rx = (uct_rc_iface_cleanup_rx_func_t)
            ucs_empty_function_return_success,
};

static uct_iface_ops_t uct_rc_gdaki_iface_tl_ops = {
    .ep_flush          = uct_base_ep_flush,
    .ep_fence          = uct_base_ep_fence,
    .ep_create         = UCS_CLASS_NEW_FUNC_NAME(uct_rc_gdaki_ep_t),
    .ep_destroy        = UCS_CLASS_DELETE_FUNC_NAME(uct_rc_gdaki_ep_t),
    .ep_get_address    = uct_rc_gdaki_ep_get_address,
    .ep_connect_to_ep  = uct_base_ep_connect_to_ep,
    .ep_pending_purge  = (uct_ep_pending_purge_func_t)ucs_empty_function,
    .iface_close       = UCS_CLASS_DELETE_FUNC_NAME(uct_rc_gdaki_iface_t),
    .iface_query       = uct_rc_gdaki_iface_query,
    .iface_get_address = uct_rc_gdaki_iface_get_address,
    .iface_get_device_address = uct_ib_iface_get_device_address,
    .iface_is_reachable       = uct_base_iface_is_reachable,
    .iface_flush              = (uct_iface_flush_func_t)
            ucs_empty_function_return_success,
    .iface_fence              = (uct_iface_fence_func_t)
            ucs_empty_function_return_unsupported,
    .iface_progress_enable    = (uct_iface_progress_enable_func_t)
            ucs_empty_function_return_unsupported,
    .iface_progress_disable   = (uct_iface_progress_disable_func_t)
            ucs_empty_function_return_unsupported,
    .iface_progress           = (uct_iface_progress_func_t)
            ucs_empty_function_return_unsupported,
};

static ucs_status_t
uct_rc_gdaki_iface_init_err_events(uct_rc_gdaki_iface_t *iface)
{
    uct_ib_mlx5_md_t *md      = uct_ib_mlx5_iface_md(&iface->super.super.super);
    uct_priv_worker_t *worker = iface->super.super.super.super.worker;
    struct mlx5dv_devx_event_channel *channel;
    ucs_status_t status;

    ucs_list_head_init(&iface->ep_list);
    iface->err_event_channel = NULL;

    if (worker->async == NULL) {
        ucs_diag("%s: no async context, device QP errors will not be "
                 "reported", uct_ib_device_name(&md->super.dev));
        return UCS_OK;
    }

    channel = mlx5dv_devx_create_event_channel(
            md->super.dev.ibv_context,
            MLX5_IB_UAPI_DEVX_CR_EV_CH_FLAGS_OMIT_DATA);
    if (channel == NULL) {
        ucs_diag("%s: mlx5dv_devx_create_event_channel() failed: %m, device "
                 "QP errors will not be reported",
                 uct_ib_device_name(&md->super.dev));
        return UCS_OK;
    }

    status = ucs_sys_fcntl_modfl(channel->fd, O_NONBLOCK, 0);
    if (status != UCS_OK) {
        goto err_destroy_channel;
    }

    iface->err_event_channel = channel;
    status = ucs_async_set_event_handler(worker->async->mode, channel->fd,
                                         UCS_EVENT_SET_EVREAD,
                                         uct_rc_gdaki_iface_err_event_handler,
                                         iface, worker->async);
    if (status != UCS_OK) {
        iface->err_event_channel = NULL;
        goto err_destroy_channel;
    }

    return UCS_OK;

err_destroy_channel:
    mlx5dv_devx_destroy_event_channel(channel);
    return status;
}

static void
uct_rc_gdaki_iface_remove_err_handler(uct_rc_gdaki_iface_t *iface)
{
    if (iface->err_event_channel != NULL) {
        ucs_async_remove_handler(iface->err_event_channel->fd, 1);
    }
}

static void
uct_rc_gdaki_iface_destroy_err_channel(uct_rc_gdaki_iface_t *iface)
{
    if (iface->err_event_channel != NULL) {
        mlx5dv_devx_destroy_event_channel(iface->err_event_channel);
        iface->err_event_channel = NULL;
    }
}

static UCS_CLASS_INIT_FUNC(uct_rc_gdaki_iface_t, uct_md_h tl_md,
                           uct_worker_h worker,
                           const uct_iface_params_t *params,
                           const uct_iface_config_t *tl_config)
{
    uct_rc_gdaki_iface_config_t *config =
            ucs_derived_of(tl_config, uct_rc_gdaki_iface_config_t);
    uct_ib_mlx5_md_t *md = ucs_derived_of(tl_md, uct_ib_mlx5_md_t);
    uct_ib_iface_init_attr_t init_attr = {};
    uct_md_mem_reg_params_t reg_params = {};
    UCS_STRING_BUFFER_ONSTACK(strb, 64);
    char *gpu_name, *ib_name;
    char pci_addr[UCS_SYS_BDF_NAME_MAX];
    ucs_status_t status;
    int cuda_id;
    int ret;
    CUdevice cuda_dev;

    if (config->num_channels > UINT8_MAX + 1) {
         ucs_error("num_channels exceeds maximum value of 256");
         return UCS_ERR_INVALID_PARAM;
    }

    self->num_channels = ucs_roundup_pow2(config->num_channels);
    self->ep_alloc_mode = config->ep_alloc_mode;

    status = uct_rc_mlx5_dp_ordering_ooo_init(md, &self->super,
                                              md->dp_ordering_cap_devx.rc,
                                              md->ddp_support_dv.rc,
                                              &config->mlx5, "rc_gda");
    if (status != UCS_OK) {
        return status;
    }

    ucs_string_buffer_appendf(&strb, "%s", params->mode.device.dev_name);
    gpu_name = ucs_string_buffer_next_token(&strb, NULL, "-");
    ib_name  = ucs_string_buffer_next_token(&strb, gpu_name, "-");

    init_attr.seg_size      = config->super.super.seg_size;
    init_attr.qp_type       = IBV_QPT_RC;
    init_attr.dev_name      = ib_name;
    init_attr.max_rd_atomic = IBV_DEV_ATTR(&md->super.dev, max_qp_rd_atom);

    UCS_CLASS_CALL_SUPER_INIT(uct_rc_mlx5_iface_common_t,
                              &uct_rc_gdaki_iface_tl_ops,
                              &uct_rc_gdaki_internal_ops, tl_md, worker, params,
                              &config->super, &config->mlx5, &init_attr);

    if (memcmp(gpu_name, UCT_DEVICE_CUDA_NAME, UCT_DEVICE_CUDA_NAME_LEN)) {
        ucs_error("wrong device name: %s\n", gpu_name);
        return status;
    }

    cuda_id = atoi(gpu_name + UCT_DEVICE_CUDA_NAME_LEN);
    status  = UCT_CUDADRV_FUNC_LOG_ERR(
            cuDeviceGetPCIBusId(pci_addr, UCS_SYS_BDF_NAME_MAX, cuda_id));
    if (status != UCS_OK) {
        return status;
    }

    status = UCT_CUDADRV_FUNC_LOG_ERR(cuDeviceGet(&cuda_dev, cuda_id));
    if (status != UCS_OK) {
        return status;
    }

    ret = ucs_posix_memalign((void**)&self->atomic_buff,
                             UCS_SYS_CACHE_LINE_SIZE, sizeof(uint64_t),
                             "gdaki_atomic_buf");
    if (ret != 0) {
        ucs_error("failed to allocate AMO buffer");
        return UCS_ERR_NO_MEMORY;
    }

    status = uct_ib_reg_mr(&md->super, self->atomic_buff, sizeof(uint64_t),
                           &reg_params, UCT_IB_MEM_ACCESS_FLAGS, NULL,
                           &self->atomic_mr);
    if (status != UCS_OK) {
        goto err_atomic_buff;
    }

    if (pthread_mutex_init(&self->ep_init_lock, NULL) != 0) {
        status = UCS_ERR_IO_ERROR;
        goto err_lock;
    }

    /* Before the channel pool, since its CQs are subscribed to the channel */
    status = uct_rc_gdaki_iface_init_err_events(self);
    if (status != UCS_OK) {
        goto err_err_events;
    }

    if (self->ep_alloc_mode == UCT_RC_GDAKI_EP_ALLOC_MODE_POOL) {
        status = uct_rc_gdaki_iface_init_channel_pool(self, config);
        if (status != UCS_OK) {
            goto err_pool;
        }
    }

    self->uar_wrap               = NULL;
    self->cuda.dev               = cuda_dev;
    self->cuda.ctx               = NULL;
    self->cuda.alloc_granularity = SIZE_MAX;
    self->failover.stream        = NULL;
    self->failover.buf           = NULL;
    self->failover.buf_size      = 0;

    return UCS_OK;

err_pool:
    uct_rc_gdaki_iface_remove_err_handler(self);
    uct_rc_gdaki_iface_destroy_err_channel(self);
err_err_events:
    pthread_mutex_destroy(&self->ep_init_lock);
err_lock:
    ibv_dereg_mr(self->atomic_mr);
err_atomic_buff:
    ucs_free(self->atomic_buff);
    return status;
}

static UCS_CLASS_CLEANUP_FUNC(uct_rc_gdaki_iface_t)
{
    uct_rc_gdaki_iface_remove_err_handler(self);
    pthread_mutex_destroy(&self->ep_init_lock);
    ibv_dereg_mr(self->atomic_mr);
    ucs_free(self->atomic_buff);
    if (self->ep_alloc_mode == UCT_RC_GDAKI_EP_ALLOC_MODE_POOL) {
        uct_rc_gdaki_iface_cleanup_channel_pool(self);
    }

    /* After the channel CQs are destroyed */
    uct_rc_gdaki_iface_destroy_err_channel(self);

    if (self->uar_wrap != NULL) {
        uct_worker_tl_data_put(self->uar_wrap, uct_rc_gdaki_devx_uar_cleanup);
        self->uar_wrap = NULL;
    }

    uct_rc_gdaki_iface_failover_cleanup(self);

    if (self->cuda.ctx != NULL) {
        (void)UCT_CUDADRV_FUNC_LOG_WARN(
                cuDevicePrimaryCtxRelease(self->cuda.dev));
    }
}

UCS_CLASS_DEFINE(uct_rc_gdaki_iface_t, uct_rc_mlx5_iface_common_t);

static UCS_CLASS_DEFINE_NEW_FUNC(uct_rc_gdaki_iface_t, uct_iface_t, uct_md_h,
                                 uct_worker_h, const uct_iface_params_t*,
                                 const uct_iface_config_t*);

static UCS_CLASS_DEFINE_DELETE_FUNC(uct_rc_gdaki_iface_t, uct_iface_t);

typedef struct {
    unsigned               ibdev_index;
    ucs_sys_dev_distance_t dist;
    int                    usecount;
    int                    reachable;
    unsigned               plane; /* Plane rank, or UCT_GDAKI_PLANE_NONE */
} uct_gdaki_dev_score_t;

typedef struct {
    ucs_sys_device_t sys_dev;
    uint64_t         cuda_map;
    int              direct_nic;
} uct_gdaki_dev_matrix_elem_t;

typedef struct {
    ucs_sys_device_t sys_dev;
    ucs_sys_bus_id_t bus_id;
    int              cuda_idx; /* CUDA driver index, -1 if not CUDA-visible */
} uct_gdaki_gpu_info_t;

static const uct_gdaki_gpu_info_t *
uct_gdaki_gpu_bus_id_lookup(const uct_gdaki_gpu_info_t *gpus, unsigned count,
                            const ucs_sys_bus_id_t *bus_id)
{
    ucs_bus_id_bit_rep_t bus_id_key = ucs_topo_get_bus_id_bit_repr(bus_id);
    unsigned gpu_idx;

    for (gpu_idx = 0; gpu_idx < count; gpu_idx++) {
        if (bus_id_key ==
            ucs_topo_get_bus_id_bit_repr(&gpus[gpu_idx].bus_id)) {
            return &gpus[gpu_idx];
        }
    }

    return NULL;
}

static int uct_gdaki_dev_matrix_score(const void *pa, const void *pb, void *arg)
{
    const uct_gdaki_dev_score_t *a = pa;
    const uct_gdaki_dev_score_t *b = pb;
    int result;

    /* Sort reachable devices before unreachable ones */
    if (a->reachable != b->reachable) {
        return b->reachable - a->reachable;
    }

    /* Prefer lower latency device, and if same latency prefer the less utilized */
    result = (ucs_fp_compare(a->dist.latency, b->dist.latency) *
              UCT_GDAKI_MAX_CUDA_DEVICES) +
             ucs_signum(a->usecount - b->usecount);
    if (result != 0) {
        return result;
    }

    /* Break the remaining ties by IB device index, so that equally close and
     * equally used devices are picked in a deterministic order, which keeps the
     * devices of one NIC together */
    return ucs_signum((int64_t)a->ibdev_index - b->ibdev_index);
}

/* Assign a device to a GPU */
static void
uct_gdaki_dev_matrix_select(const uct_gdaki_gpu_info_t *gpu,
                            uct_gdaki_dev_matrix_elem_t *dmat,
                            uct_gdaki_dev_score_t *score)
{
    if (gpu->cuda_idx >= 0) {
        dmat[score->ibdev_index].cuda_map |= UCS_BIT(gpu->cuda_idx);
    }

    score->usecount++;
}

static ucs_status_t
uct_gdaki_get_cuda_sys_dev_and_bus_id(int cuda_idx, ucs_sys_device_t *sys_dev_p,
                                      ucs_sys_bus_id_t *bus_id_p)
{
    ucs_status_t status;
    CUdevice cuda_dev;

    status = UCT_CUDADRV_FUNC_LOG_ERR(cuDeviceGet(&cuda_dev, cuda_idx));
    if (status != UCS_OK) {
        return status;
    }

    return uct_cuda_get_sys_dev_and_bus_id(cuda_dev, sys_dev_p, bus_id_p);
}

static ucs_status_t
uct_gdaki_enum_gpus(uct_gdaki_gpu_info_t *gpus, unsigned *count_p)
{
    uct_gdaki_gpu_info_t cuda_gpus[UCT_GDAKI_MAX_CUDA_DEVICES];
    const uct_gdaki_gpu_info_t *gpu_info;
    unsigned nvml_dev_count, nvml_idx, gpu_count, cuda_gpu_count;
    int cuda_dev_count, cuda_idx;
    ucs_sys_bus_id_t bus_id;
    nvmlDevice_t nvml_dev;
    nvmlPciInfo_t nvml_pci;
    ucs_sys_device_t sys_dev;
    ucs_status_t status;

    status = UCT_CUDADRV_FUNC_LOG_ERR(cuDeviceGetCount(&cuda_dev_count));
    if (status != UCS_OK) {
        return status;
    }

    ucs_assert_always(cuda_dev_count <= UCT_GDAKI_MAX_CUDA_DEVICES);

    cuda_gpu_count = 0;
    for (cuda_idx = 0; cuda_idx < cuda_dev_count; cuda_idx++) {
        status = uct_gdaki_get_cuda_sys_dev_and_bus_id(cuda_idx, &sys_dev,
                                                       &bus_id);
        if (status != UCS_OK) {
            return status;
        }

        if (uct_gdaki_gpu_bus_id_lookup(cuda_gpus, cuda_gpu_count,
                                        &bus_id) != NULL) {
            ucs_debug("skip cuda device %d with duplicate "
                      "bdf " UCS_SYS_BUS_ID_FMT,
                      cuda_idx, UCS_SYS_BUS_ID_ARG(&bus_id));
            /* Same BDF. TODO: support MLOPart. */
            continue;
        }

        cuda_gpus[cuda_gpu_count].sys_dev  = sys_dev;
        cuda_gpus[cuda_gpu_count].bus_id   = bus_id;
        cuda_gpus[cuda_gpu_count].cuda_idx = cuda_idx;
        cuda_gpu_count++;
    }

    status = UCT_CUDA_NVML_WRAP_CALL(nvmlDeviceGetCount_v2, &nvml_dev_count);
    if (status != UCS_OK) {
        ucs_diag("NVML unavailable: using legacy CUDA-only enumeration");
        memcpy(gpus, cuda_gpus, cuda_gpu_count * sizeof(*gpus));
        *count_p = cuda_gpu_count;
        return UCS_OK;
    }

    ucs_assert_always(nvml_dev_count <= UCT_GDAKI_MAX_CUDA_DEVICES);

    /* Add non-CUDA-visible devices from NVML. TODO: support MLOPart. */
    gpu_count = 0;
    for (nvml_idx = 0; nvml_idx < nvml_dev_count; nvml_idx++) {
        status = UCT_CUDA_NVML_WRAP_CALL(nvmlDeviceGetHandleByIndex, nvml_idx,
                                         &nvml_dev);
        if (status != UCS_OK) {
            return status;
        }

        status = UCT_CUDA_NVML_WRAP_CALL(nvmlDeviceGetPciInfo_v3, nvml_dev,
                                         &nvml_pci);
        if (status != UCS_OK) {
            return status;
        }

        bus_id.domain   = nvml_pci.domain;
        bus_id.bus      = nvml_pci.bus;
        bus_id.slot     = nvml_pci.device;
        bus_id.function = 0;

        gpu_info = uct_gdaki_gpu_bus_id_lookup(cuda_gpus, cuda_gpu_count,
                                               &bus_id);
        if (gpu_info != NULL) {
            gpus[gpu_count++] = *gpu_info;
            /* Already added as CUDA-visible. */
            continue;
        }

        status = ucs_topo_find_device_by_bus_id(&bus_id, &sys_dev);
        if (status != UCS_OK) {
            return status;
        }

        if (gpu_count == UCT_GDAKI_MAX_CUDA_DEVICES) {
            ucs_error("exceeded maximal number of GDAKI CUDA devices (%u)",
                      UCT_GDAKI_MAX_CUDA_DEVICES);
            return UCS_ERR_EXCEEDS_LIMIT;
        }

        gpus[gpu_count].sys_dev  = sys_dev;
        gpus[gpu_count].bus_id   = bus_id;
        gpus[gpu_count].cuda_idx = -1;
        gpu_count++;
    }

    *count_p = gpu_count;
    return UCS_OK;
}

/* IPv4 network which a device routes to, identifying its fabric plane. All the
 * networks are assumed to have the same prefix length, so the address alone
 * identifies a network. */
typedef struct {
    uint32_t addr; /* Network address, without the host bits, 0 for none */
    unsigned dev;  /* Index of the active device */
} uct_gdaki_fabric_route_t;

UCS_ARRAY_DECLARE_TYPE(uct_gdaki_fabric_routes_t, unsigned,
                       uct_gdaki_fabric_route_t);

typedef struct {
    uct_gdaki_fabric_routes_t *routes;
    unsigned                  dev;
    ucs_status_t              status;
} uct_gdaki_fabric_collect_t;

static int
uct_gdaki_fabric_route_compare(const void *pa, const void *pb, void *arg)
{
    const uct_gdaki_fabric_route_t *a = pa;
    const uct_gdaki_fabric_route_t *b = pb;

    if (a->addr != b->addr) {
        return ucs_signum((int64_t)a->addr - b->addr);
    }

    return ucs_signum((int64_t)a->dev - b->dev);
}

/**
 * Group devices into fabric planes by the networks they route to.
 *
 * The plane of a device is named by the lowest network which it shares with
 * enough devices to be a plane: at least one device for each GPU, but not all
 * the devices, since a network of all of them, like a summary route, cannot
 * tell them apart. Networks which fewer devices share are not planes.
 * Devices of the same name are on the same plane, and devices which have no
 * name are not on a plane. Planes are used only if there are at least two of
 * them: with a single plane there is nothing to balance, and with none the
 * topology is not known. In both cases the caller keeps assigning devices by
 * PCIe distance only.
 *
 * Planes are ranked by their network, which is the same on all hosts of a
 * fabric. GPUs which take the first planes in rank order therefore always have
 * planes in common, also across hosts.
 *
 * @param [in,out] routes      Routes of the devices, sorted by this function.
 * @param [in]     num_routes  Number of routes.
 * @param [in]     num_devs    Number of active devices.
 * @param [in]     num_gpus    Number of GPUs, including the ones not visible.
 * @param [out]    plane       Plane rank of each device, or
 *                             UCT_GDAKI_PLANE_NONE if it cannot be used.
 * @param [out]    auto_k_p    Number of devices to assign to each GPU when it
 *                             is not configured explicitly.
 *
 * @return Number of planes, or 0 if the devices should not be grouped, in which
 *         case the outputs are not valid.
 */
static unsigned
uct_gdaki_fabric_classify(uct_gdaki_fabric_route_t *routes,
                          unsigned num_routes, unsigned num_devs,
                          unsigned num_gpus, unsigned *plane,
                          unsigned long *auto_k_p)
{
    const unsigned min_devs = ucs_max(num_gpus, 2u);
    unsigned num_planes     = 0;
    unsigned long auto_k    = 0;
    uct_gdaki_fabric_route_t *names, *name;
    unsigned i, j, k, num_run_devs;

    ucs_assert(num_gpus > 0);

    *auto_k_p = 0;

    /* The name of each device, which has no network until one is found */
    names = ucs_calloc(num_devs, sizeof(*names), "gdaki fabric names");
    if (names == NULL) {
        return 0;
    }

    for (i = 0; i < num_devs; ++i) {
        names[i].dev = i;
        plane[i]     = UCT_GDAKI_PLANE_NONE;
    }

    ucs_qsort_r(routes, num_routes, sizeof(*routes),
                uct_gdaki_fabric_route_compare, NULL);

    /* The routes of one network are adjacent and sorted by device */
    for (i = 0; i < num_routes; i = j) {
        num_run_devs = 1;
        j            = i + 1;
        while ((j < num_routes) && (routes[j].addr == routes[i].addr)) {
            num_run_devs += (routes[j].dev != routes[j - 1].dev);
            ++j;
        }

        if ((num_run_devs < min_devs) || (num_run_devs >= num_devs)) {
            continue;
        }

        /* The networks come in ascending order, so the lowest one stays */
        for (k = i; k < j; ++k) {
            ucs_assert(routes[k].dev < num_devs);
            name = &names[routes[k].dev];
            if (name->addr == 0) {
                name->addr = routes[i].addr;
            }
        }
    }

    /* Devices of the same name are a plane, if there is one for each GPU. The
     * names are sorted, so the planes are ranked by their network. */
    ucs_qsort_r(names, num_devs, sizeof(*names), uct_gdaki_fabric_route_compare,
                NULL);
    for (i = 0; i < num_devs; i = j) {
        j = i + 1;
        while ((j < num_devs) && (names[j].addr == names[i].addr)) {
            ++j;
        }

        if ((names[i].addr == 0) || ((j - i) < num_gpus)) {
            continue;
        }

        for (k = i; k < j; ++k) {
            plane[names[k].dev] = num_planes;
        }

        auto_k += (j - i) / num_gpus;
        num_planes++;
    }

    ucs_free(names);
    if (num_planes < 2) {
        return 0;
    }

    *auto_k_p = auto_k;
    return num_planes;
}

/* Find out the network device of a RoCE device while its context is open.
 * Returns 0 if the device is not a RoCE one, or its network device is not
 * found. */
static unsigned
uct_gdaki_dev_ndev_index(uct_ib_device_t *dev)
{
    uint8_t port_num = dev->first_port;
    char ndev_name[IFNAMSIZ];
    unsigned gid_index, ndev_index;

    if (!uct_ib_device_is_port_roce(dev, port_num)) {
        return 0;
    }

    /* Any valid GID of the port leads to the network device it runs over */
    for (gid_index = 0;
         gid_index < uct_ib_device_port_attr(dev, port_num)->gid_tbl_len;
         ++gid_index) {
        if ((uct_ib_device_get_roce_ndev_name(dev, port_num, gid_index,
                                              ndev_name,
                                              sizeof(ndev_name)) == UCS_OK) &&
            (ucs_ifname_to_ndev_index(ndev_name, &ndev_index) == UCS_OK)) {
            return ndev_index;
        }
    }

    return 0;
}

static void
uct_gdaki_fabric_collect_route(const struct sockaddr *dest, unsigned prefix_len,
                               uint8_t route_type, void *arg)
{
    uct_gdaki_fabric_collect_t *collect = arg;
    uct_gdaki_fabric_route_t *route;
    uint32_t addr;

    /* Only the IPv4 routes which can tell fabrics apart are used. Not the
     * default and host routes, which are not specific to a fabric, and not the
     * link-local and multicast routes, which every interface has. The
     * destination of a kernel route has no host bits, so routes to the same
     * network have the same address. */
    if ((route_type != RTN_UNICAST) || (dest->sa_family != AF_INET) ||
        (prefix_len >= 32)) {
        return;
    }

    addr = ntohl(UCS_SOCKET_INET_ADDR(dest).s_addr);
    if ((addr == 0) || ((addr & 0xffff0000) == 0xa9fe0000) ||
        IN_MULTICAST(addr) || IN_BADCLASS(addr)) {
        return;
    }

    route = ucs_array_append(collect->routes,
                             collect->status = UCS_ERR_NO_MEMORY; return);
    route->addr = addr;
    route->dev  = collect->dev;
}

/*
 * Group the active devices by fabric plane, and set the plane of each score. A
 * device which is not on a plane gets UCT_GDAKI_PLANE_NONE. The planes are
 * found by the routes of the network devices of RoCE devices only.
 *
 * Returns the number of planes, or 0 if the devices are not grouped, in which
 * case the scores are left as they are.
 */
static unsigned
uct_gdaki_fabric_detect(const unsigned *ndev_indexes, unsigned num_devs,
                        unsigned num_gpus, uct_gdaki_dev_score_t *scores,
                        unsigned long *auto_k_p)
{
    uct_gdaki_fabric_collect_t collect;
    uct_gdaki_fabric_routes_t routes;
    unsigned num_planes = 0;
    unsigned *plane;
    unsigned i;

    plane = ucs_calloc(num_devs, sizeof(*plane), "gdaki dev planes");
    if (plane == NULL) {
        return 0;
    }

    ucs_array_init_dynamic(&routes);
    collect.routes = &routes;
    collect.status = UCS_OK;

    for (i = 0; i < num_devs; ++i) {
        /* A device without a network device, such as an InfiniBand one, has no
         * routes to find its plane by: keep assigning by PCIe distance only */
        if (ndev_indexes[i] == 0) {
            goto out;
        }

        collect.dev = i;
        ucs_netlink_foreach_route(ndev_indexes[i],
                                  uct_gdaki_fabric_collect_route, &collect);
    }

    if (collect.status != UCS_OK) {
        goto out;
    }

    num_planes = uct_gdaki_fabric_classify(ucs_array_begin(&routes),
                                           ucs_array_length(&routes), num_devs,
                                           num_gpus, plane, auto_k_p);
    if (num_planes == 0) {
        goto out;
    }

    ucs_debug("detected %u fabric planes among %u active IB devices",
              num_planes, num_devs);
    for (i = 0; i < num_devs; ++i) {
        scores[i].plane = plane[i];
    }

out:
    ucs_array_cleanup_dynamic(&routes);
    ucs_free(plane);
    return num_planes;
}

static uct_gdaki_dev_matrix_elem_t *
uct_gdaki_dev_matrix_init(const uct_ib_md_t *ib_md, size_t *dmat_length_p)
{
    unsigned long ib_per_cuda         = ib_md->config.gda_max_hca_per_gpu;
    uct_gdaki_dev_matrix_elem_t *dmat = NULL;
    int num_sys_active_ibdevs         = 0;
    unsigned long fabric_ib_per_cuda  = 0;
    unsigned num_planes               = 0;
    uct_gdaki_gpu_info_t gpus[UCT_GDAKI_MAX_CUDA_DEVICES];
    ucs_status_t status;
    int ibdev_index, ibdev_count, score_index;
    unsigned long num_gpu_reachable_ibdevs, num_plane_selected_ibdevs;
    unsigned long plane_ib_per_cuda;
    unsigned gpu_count, gpu_index, num_used_planes, plane;
    struct ibv_device **device_list;
    struct ibv_device *ibdev;
    uct_gdaki_dev_score_t *scores;
    unsigned *ndev_indexes;
    uct_gdaki_dev_matrix_elem_t *ibdesc;
    struct ibv_context *context;

    /* Obtain the list of IB devices */
    device_list = ibv_get_device_list(&ibdev_count);
    if (device_list == NULL) {
        return NULL;
    }

    ucs_assert(ibdev_count > 0);
    /* Allocate memory for device matrix */
    dmat = ucs_calloc(ibdev_count, sizeof(*dmat), "dev matrix");
    if (dmat == NULL) {
        status = UCS_ERR_NO_MEMORY;
        goto out_dev;
    }

    /* Allocate memory for device scores */
    scores = ucs_calloc(ibdev_count, sizeof(*scores), "dev scores");
    if (scores == NULL) {
        status = UCS_ERR_NO_MEMORY;
        goto out_dmat;
    }

    /* Allocate memory for network device indexes */
    ndev_indexes = ucs_calloc(ibdev_count, sizeof(*ndev_indexes),
                              "dev ndev indexes");
    if (ndev_indexes == NULL) {
        status = UCS_ERR_NO_MEMORY;
        goto out_scores;
    }

    /* Query each IB device so its sys_dev is resolved and its port state is
     * known before direct-NIC sibling matching can be triggered. */
    for (ibdev_index = 0; ibdev_index < ibdev_count; ibdev_index++) {
        uct_ib_device_t tmp_dev = {0};

        ibdesc  = &dmat[ibdev_index];
        ibdev   = device_list[ibdev_index];
        context = ibv_open_device(ibdev);
        if (context == NULL) {
            ucs_error("ibv_open_device(%s) failed: %m",
                      ibv_get_device_name(ibdev));
            status = UCS_ERR_IO_ERROR;
            goto out;
        }

        tmp_dev.ibv_context = context;
        status              = uct_ib_device_query(&tmp_dev, ibdev);
        if (status != UCS_OK) {
            ibv_close_device(context);
            goto out;
        }

        ibdesc->sys_dev = tmp_dev.sys_dev;
        if (uct_ib_device_port_attr(&tmp_dev, tmp_dev.first_port)->state !=
            IBV_PORT_ACTIVE) {
            ucs_debug("%s:%u: skipping GDA device, port is not active",
                      uct_ib_device_name(&tmp_dev), tmp_dev.first_port);
            ibv_close_device(context);
            continue;
        }

        ibdesc->direct_nic = uct_ib_mlx5dv_check_direct_nic(&tmp_dev, 1) !=
                             UCS_SYS_DEVICE_ID_UNKNOWN;
        ndev_indexes[num_sys_active_ibdevs] =
                uct_gdaki_dev_ndev_index(&tmp_dev);
        scores[num_sys_active_ibdevs++].ibdev_index = ibdev_index;
        ibv_close_device(context);
    }

    if (num_sys_active_ibdevs == 0) {
        ucs_debug("no active IB devices found for GDA");
        status = UCS_ERR_NO_DEVICE;
        goto out;
    }

    /* Enumerate physical GPUs via NVML (or CUDA fallback). NVML is used here
     * so that CUDA_VISIBLE_DEVICES does not hide GPUs from the topology
     * calculation */
    status = uct_gdaki_enum_gpus(gpus, &gpu_count);
    if (status != UCS_OK) {
        goto out;
    }

    if (gpu_count == 0) {
        ucs_debug("no GPU devices found for GDA");
        status = UCS_ERR_NO_DEVICE;
        goto out;
    }

    /* The fabric planes are needed to give all GPUs the same planes, even if
     * the number of devices per GPU is configured explicitly */
    if (ib_md->config.gda_fabric_detect) {
        num_planes = uct_gdaki_fabric_detect(ndev_indexes,
                                             num_sys_active_ibdevs, gpu_count,
                                             scores, &fabric_ib_per_cuda);
    }

    if (ib_per_cuda == UCS_ULUNITS_AUTO) {
        ib_per_cuda = (num_planes > 0) ?
                      fabric_ib_per_cuda :
                      ucs_max(1ul,
                              (unsigned long)num_sys_active_ibdevs / gpu_count);
    }

    /* A GPU gets the same number of devices from each of the first planes, in
     * the rank order. Without planes there is a single one, which has all the
     * devices. */
    num_used_planes   = ucs_max(ucs_min(num_planes, ib_per_cuda), 1ul);
    plane_ib_per_cuda = ib_per_cuda / num_used_planes;

    /* Map each GPU to the best suited IB devices */
    for (gpu_index = 0; gpu_index < gpu_count; gpu_index++) {
        /* Update PCI distance in IB device scores */
        num_gpu_reachable_ibdevs = 0;
        for (score_index = 0; score_index < num_sys_active_ibdevs;
             score_index++) {
            ibdesc = &dmat[scores[score_index].ibdev_index];
            /* A device which is not on a plane is not used */
            scores[score_index].reachable =
                    (scores[score_index].plane != UCT_GDAKI_PLANE_NONE) &&
                    ucs_topo_is_reachable(ibdesc->sys_dev,
                                          gpus[gpu_index].sys_dev);
            if (!scores[score_index].reachable) {
                continue;
            }

            num_gpu_reachable_ibdevs++;
            status = ucs_topo_get_distance(gpus[gpu_index].sys_dev,
                                           ibdesc->sys_dev,
                                           &scores[score_index].dist);
            if (status != UCS_OK) {
                goto out;
            }
        }

        if (num_gpu_reachable_ibdevs == 0) {
            ucs_debug("GPU " UCS_SYS_BUS_ID_FMT
                      " has no reachable active IB devices for GDA",
                      UCS_SYS_BUS_ID_ARG(&gpus[gpu_index].bus_id));
            continue;
        }

        /* Sort and select the best suited IB devices for this GPU */
        ucs_qsort_r(scores, num_sys_active_ibdevs, sizeof(*scores),
                    uct_gdaki_dev_matrix_score, NULL);

        /* Direct NIC is closest and can be only one */
        if (dmat[scores[0].ibdev_index].direct_nic) {
            uct_gdaki_dev_matrix_select(&gpus[gpu_index], dmat, &scores[0]);
            continue;
        }

        /* Select the best suited devices of each used plane. Reachable
         * devices are sorted first, so the loops select only devices
         * reachable from this GPU */
        for (plane = 0; plane < num_used_planes; plane++) {
            num_plane_selected_ibdevs = 0;
            for (score_index = 0;
                 (score_index < num_sys_active_ibdevs) &&
                 scores[score_index].reachable &&
                 (num_plane_selected_ibdevs < plane_ib_per_cuda);
                 score_index++) {
                if (scores[score_index].plane == plane) {
                    uct_gdaki_dev_matrix_select(&gpus[gpu_index], dmat,
                                                &scores[score_index]);
                    num_plane_selected_ibdevs++;
                }
            }
        }
    }

    /* Output processed device matrix length */
    *dmat_length_p = ibdev_count;

out:
    ucs_free(ndev_indexes);
out_scores:
    ucs_free(scores);
out_dmat:
    if (status != UCS_OK) {
        ucs_free(dmat);
        dmat = NULL;
    }
out_dev:
    ibv_free_device_list(device_list);
    return dmat;
}

ucs_status_t
uct_gdaki_fill_cuda_tl_devices(const uct_ib_md_t *ib_md,
                               uct_tl_device_resource_t **tl_devices_p,
                               unsigned *num_tl_devices_p)
{
    static ucs_init_once_t once = UCS_INIT_ONCE_INITIALIZER;
    static uct_gdaki_dev_matrix_elem_t *dmat;
    static size_t dmat_length;
    const uct_gdaki_dev_matrix_elem_t *ibdesc;
    uct_tl_device_resource_t *tl_devices;
    unsigned num_tl_devices;
    ucs_status_t status;
    CUdevice device;
    int i;

    UCS_INIT_ONCE(&once) {
        dmat = uct_gdaki_dev_matrix_init(ib_md, &dmat_length);
    }

    if (dmat == NULL) {
        ucs_debug("%s: device matrix initialization failed",
                  uct_ib_device_name(&ib_md->dev));
        return UCS_ERR_NO_DEVICE;
    }

    for (ibdesc = dmat; ibdesc - dmat < dmat_length; ibdesc++) {
        if (ibdesc->sys_dev == ib_md->dev.sys_dev) {
            break;
        }
    }

    if (ibdesc - dmat == dmat_length) {
        ucs_debug("%s: IB device not found in device matrix",
                  uct_ib_device_name(&ib_md->dev));
        return UCS_ERR_NO_DEVICE;
    }

    if (ibdesc->cuda_map == 0) {
        ucs_debug("%s: no assigned GPU found", uct_ib_device_name(&ib_md->dev));
        return UCS_ERR_NO_DEVICE;
    }

    tl_devices = ucs_malloc(sizeof(*tl_devices) *
                                    ucs_popcount(ibdesc->cuda_map),
                            "gdaki_tl_devices");
    if (tl_devices == NULL) {
        return UCS_ERR_NO_MEMORY;
    }

    num_tl_devices = 0;
    ucs_for_each_bit(i, ibdesc->cuda_map) {
        status = UCT_CUDADRV_FUNC_LOG_ERR(cuDeviceGet(&device, i));
        if (status != UCS_OK) {
            goto err;
        }

        snprintf(tl_devices[num_tl_devices].name,
                 sizeof(tl_devices[num_tl_devices].name), "%s%d-%s:%d",
                 UCT_DEVICE_CUDA_NAME, device, uct_ib_device_name(&ib_md->dev),
                 ib_md->dev.first_port);
        tl_devices[num_tl_devices].type       = UCT_DEVICE_TYPE_NET;
        tl_devices[num_tl_devices].sys_device = ib_md->dev.sys_dev;
        num_tl_devices++;
    }

    *num_tl_devices_p = num_tl_devices;
    *tl_devices_p     = tl_devices;
    return UCS_OK;

err:
    ucs_free(tl_devices);
    return status;
}

static ucs_status_t
uct_gdaki_query_tl_devices(uct_md_h tl_md,
                           uct_tl_device_resource_t **tl_devices_p,
                           unsigned *num_tl_devices_p)
{
    uct_ib_mlx5_md_t *ib_mlx5_md = ucs_derived_of(tl_md, uct_ib_mlx5_md_t);
    uct_ib_md_t *ib_md           = &ib_mlx5_md->super;

    if (!(uct_gdaki_get_driver_features(ib_md) & UCT_GDAKI_SUPPORTED)) {
        ucs_debug("%s: GDAKI is not supported",
                  uct_ib_device_name(&ib_md->dev));
        return UCS_ERR_NO_DEVICE;
    }

    return uct_gdaki_fill_cuda_tl_devices(ib_md, tl_devices_p,
                                          num_tl_devices_p);
}

UCT_TL_DEFINE_ENTRY(&uct_ib_component, rc_gda, uct_gdaki_query_tl_devices,
                    uct_rc_gdaki_iface_t, "RC_GDA_",
                    uct_rc_gdaki_iface_config_table,
                    uct_rc_gdaki_iface_config_t);

UCT_TL_INIT(&uct_ib_component, rc_gda, ctor, , )
