/**
 * Copyright (c) NVIDIA CORPORATION & AFFILIATES, 2025. ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#ifndef UCT_GDAKI_H
#define UCT_GDAKI_H

#include <uct/ib/mlx5/rc/rc_mlx5_common.h>
#include <uct/base/uct_iface.h>
#include <uct/cuda/base/cuda_mem.h>
#include <ucs/datastruct/mpool.h>

#include <cuda.h>
#include <pthread.h>

#include "gdaki_dev.h"

#define UCT_DEVICE_CUDA_NAME     "cuda"
#define UCT_DEVICE_CUDA_NAME_LEN 4

typedef struct {
    uct_ib_mlx5_cq_t   cq;
    uct_ib_mlx5_txwq_t qp;
    /* With failover, the ready WQE index when the channel was moved to
     * another endpoint, and the WQE index on that endpoint after the moved
     * WQEs, UINT64_MAX if none */
    uint64_t           moved_ready;
    uint64_t           moved_end;
} uct_rc_gdaki_channel_t;

typedef struct {
    uct_worker_tl_data_t   super;
    uct_ib_mlx5_devx_uar_t *uar;
    CUdeviceptr            device_ptr;
} uct_rc_gdaki_devx_uar_t;

typedef struct {
    uintptr_t              gpu_ptr;
    uct_rc_gdaki_channel_t channels[0];
} uct_rc_gdaki_channel_block_t;

typedef struct {
    void                    *gpu_mem;
    uct_cuda_mem_t          gpu_raw;
    struct mlx5dv_devx_umem *umem;
} uct_rc_gdaki_channel_block_mem_t;

typedef struct uct_rc_gdaki_iface {
    uct_rc_mlx5_iface_common_t super;
    struct ibv_mr              *atomic_mr;
    uint64_t                   *atomic_buff;
    unsigned                   num_channels;
    unsigned                   ep_alloc_mode;
    pthread_mutex_t            ep_init_lock;
    ucs_mpool_t                channel_pool;
    uct_rc_gdaki_devx_uar_t    *uar_wrap;
    struct {
        CUdevice  dev;
        CUcontext ctx;
        size_t    alloc_granularity;
    } cuda;
    /* Delivers an event when a channel CQ receives an error completion */
    struct mlx5dv_devx_event_channel *err_event_channel;
    /* Endpoints which can receive error events, protected by the async lock */
    ucs_list_link_t            ep_list;
    /* Failover accesses device memory while kernels run, so it uses a
     * non-blocking stream and a pinned buffer, created before kernels run */
    struct {
        CUstream stream;
        void     *buf;
        size_t   buf_size;
    } failover;
} uct_rc_gdaki_iface_t;


/* Progress of uct_ep_failover_move() */
enum {
    UCT_RC_GDAKI_MOVE_STATE_NONE,
    UCT_RC_GDAKI_MOVE_STATE_PAUSING, /* Waiting for the device on to_ep */
    UCT_RC_GDAKI_MOVE_STATE_POSTED,  /* Waiting for the moved WQEs */
    UCT_RC_GDAKI_MOVE_STATE_DONE
};


typedef struct uct_rc_gdaki_ep {
    uct_base_ep_t                    super;
    uint8_t                          dev_ep_init;
    /* An error event was received, and reporting it was scheduled */
    uint8_t                          error_scheduled;
    /* Created with UCT_EP_PARAM_FLAG_FAILOVER */
    uint8_t                          failover;
    /* Log2 of the path MTU in bytes, set when connected */
    uint8_t                          log_path_mtu;
    /* Paused by uct_ep_failover_pause(), until moved or failed */
    uint8_t                          paused;
    /* Progress of uct_ep_failover_move() */
    uint8_t                          move_state;
    uct_rc_gdaki_channel_block_mem_t mem;
    uct_rc_gdaki_channel_block_t     *channel_block;
    ucs_list_link_t                  list;
} uct_rc_gdaki_ep_t;

ucs_status_t
uct_gdaki_fill_cuda_tl_devices(const uct_ib_md_t *ib_md,
                               uct_tl_device_resource_t **tl_devices_p,
                               unsigned *num_tl_devices_p);

#endif /* UCT_GDAKI_H */
