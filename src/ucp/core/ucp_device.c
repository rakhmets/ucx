/**
 * Copyright (c) NVIDIA CORPORATION & AFFILIATES, 2025-2026. ALL RIGHTS RESERVED.
 * Copyright (C) Advanced Micro Devices, Inc. 2026. ALL RIGHTS RESERVED.
 *
 * See file LICENSE for terms.
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <ucp/core/ucp_worker.h>
#include <ucp/core/ucp_types.h>
#include <ucp/core/ucp_mm.h>
#include <ucp/dt/dt_contig.h>
#include <ucp/api/device/ucp_host.h>
#include <ucp/api/device/ucp_device_types.h>
#include <ucs/type/param.h>
#include <ucp/wireup/wireup.h>
#include <ucp/wireup/wireup_ep.h>
#include <uct/api/v2/uct_v2.h>

#include "ucp_worker.inl"
#include "ucp_ep.inl"
#include "ucp_mm.inl"

typedef struct {
    uct_md_h md;
    void     *release_handle;
} ucp_device_mem_elem_release_handle_t;

UCS_ARRAY_DECLARE_TYPE(ucp_device_mem_elem_release_handles_t, unsigned,
                       ucp_device_mem_elem_release_handle_t);

/* Host copy of the keys of a memory list, to translate the keys of a failed
 * device lane to the keys of another lane */
typedef struct {
    void            *handle;    /* Host copy of the memory list */
    size_t          elem_size;  /* Size of an element */
    int             is_remote;  /* Remote or local memory list */
    unsigned        num_lanes;  /* Number of lanes of an element */
    ucp_rsc_index_t rscs[UCP_MAX_LANES]; /* Resource of each lane */
} ucp_device_mem_list_keys_t;

typedef struct {
    uct_allocated_memory_t                mem;
    uint32_t                              mem_list_length;
    ucp_device_mem_elem_release_handles_t release_handles;
    ucp_device_mem_list_keys_t            keys;
} ucp_device_handle_info_t;

#define ucp_device_handle_hash_key(_handle) \
    kh_int64_hash_func((uintptr_t)(_handle))
KHASH_INIT(ucp_device_handle_allocs, void*, ucp_device_handle_info_t, 1,
           ucp_device_handle_hash_key, kh_int64_hash_equal);

/* Hash to track handle allocator, used at release time */
static khash_t(ucp_device_handle_allocs) ucp_device_handle_hash;
static ucs_spinlock_t ucp_device_handle_hash_lock;

/* Size of temporary allocation for local sys_dev detection */
#define UCP_DEVICE_LOCAL_SYS_DEV_DETECT_SIZE 64

enum {
    UCP_DEVICE_TL_TYPE_FIRST,
    UCP_DEVICE_TL_TYPE_NOLKEY = UCP_DEVICE_TL_TYPE_FIRST,
    UCP_DEVICE_TL_TYPE_LKEY,
    UCP_DEVICE_TL_TYPE_LAST,
};


void ucp_device_init(void)
{
    ucs_spinlock_init(&ucp_device_handle_hash_lock, 0);
    kh_init_inplace(ucp_device_handle_allocs, &ucp_device_handle_hash);
}

void ucp_device_cleanup(void)
{
    kh_destroy_inplace(ucp_device_handle_allocs, &ucp_device_handle_hash);
    ucs_spinlock_destroy(&ucp_device_handle_hash_lock);
}

static ucs_status_t ucp_device_mem_elem_release_handles_append(
        ucp_device_mem_elem_release_handles_t *release_handles, uct_md_h md,
        void *release_handle)
{
    ucp_device_mem_elem_release_handle_t *it;

    it     = ucs_array_append(release_handles, return UCS_ERR_NO_MEMORY);
    it->md = md;
    it->release_handle = release_handle;
    return UCS_OK;
}

static void ucp_device_mem_elem_release_handles_cleanup(
        ucp_device_mem_elem_release_handles_t *release_handles)
{
    ucp_device_mem_elem_release_handle_t *it;

    /* Check buffer explicitly: Coverity issues FORWARD_NULL error;
       ucs_array_is_empty is not enough as Coverity does not correlate
       non-zero length with a non-NULL buffer after create_handle paths. */
    if (ucs_array_begin(release_handles) == NULL) {
        return;
    }

    ucs_array_for_each(it, release_handles) {
        uct_md_mem_elem_release(it->md, it->release_handle);
    }

    ucs_array_cleanup_dynamic(release_handles);
}

static ucs_status_t ucp_device_mem_handle_hash_insert(
        const uct_allocated_memory_t *mem_handle, uint32_t mem_list_length,
        ucp_device_mem_elem_release_handles_t *release_handles,
        ucp_device_mem_list_keys_t *keys)
{
    ucs_status_t status;
    khiter_t iter;
    int ret;
    ucp_device_handle_info_t info;

    info.mem             = *mem_handle;
    info.mem_list_length = mem_list_length;
    info.release_handles = *release_handles;
    info.keys            = *keys;

    ucs_spin_lock(&ucp_device_handle_hash_lock);
    iter = kh_put(ucp_device_handle_allocs, &ucp_device_handle_hash,
                  mem_handle->address, &ret);
    if (ret == UCS_KH_PUT_FAILED) {
        ucs_error("failed to hash handle=%p", mem_handle->address);
        status = UCS_ERR_NO_RESOURCE;
    } else if (ret == UCS_KH_PUT_KEY_PRESENT) {
        ucs_error("handle=%p already found in hash", mem_handle->address);
        status = UCS_ERR_ALREADY_EXISTS;
    } else {
        kh_value(&ucp_device_handle_hash, iter) = info;
        status                                  = UCS_OK;
        /* Ownership of the array and the keys moved to the hash */
        ucs_array_init_dynamic(release_handles);
        keys->handle = NULL;
    }

    ucs_spin_unlock(&ucp_device_handle_hash_lock);
    return status;
}

static ucp_device_handle_info_t ucp_device_mem_handle_hash_remove(void *handle)
{
    khiter_t iter;
    ucp_device_handle_info_t info;

    ucs_spin_lock(&ucp_device_handle_hash_lock);
    iter = kh_get(ucp_device_handle_allocs, &ucp_device_handle_hash, handle);
    ucs_assertv_always((iter != kh_end(&ucp_device_handle_hash)), "handle=%p",
                       handle);
    info = kh_value(&ucp_device_handle_hash, iter);
    kh_del(ucp_device_handle_allocs, &ucp_device_handle_hash, iter);
    ucs_spin_unlock(&ucp_device_handle_hash_lock);
    return info;
}

static ucs_status_t
ucp_device_detect_local_sys_dev(const ucp_context_h context,
                                const ucs_memory_type_t mem_type,
                                ucs_sys_device_t *local_sys_dev)
{
    ucs_memory_info_t mem_info;
    uct_allocated_memory_t detect_mem;
    ucs_status_t status;

    status = ucp_mem_do_alloc(context, NULL,
                              UCP_DEVICE_LOCAL_SYS_DEV_DETECT_SIZE,
                              UCT_MD_MEM_ACCESS_LOCAL_READ |
                              UCT_MD_MEM_ACCESS_LOCAL_WRITE,
                              mem_type, UCS_SYS_DEVICE_ID_UNKNOWN,
                              "local_sys_dev_detect", &detect_mem);
    if (status != UCS_OK) {
        ucs_error("failed to allocate memory for sys_dev detection: %s",
                  ucs_status_string(status));
        return status;
    }

    ucp_memory_detect_internal(context, detect_mem.address, detect_mem.length,
                               &mem_info);

    uct_mem_free(&detect_mem);

    if (mem_info.sys_dev == UCS_SYS_DEVICE_ID_UNKNOWN) {
        ucs_error("detected unknown local_sys_dev");
        return UCS_ERR_UNSUPPORTED;
    }

    *local_sys_dev = mem_info.sys_dev;

    ucs_trace("detected local_sys_dev=%u", *local_sys_dev);
    return UCS_OK;
}

static void
ucp_device_get_tl_bitmap(const ucp_worker_h worker,
                         ucp_tl_bitmap_t tl_bitmap[UCP_DEVICE_TL_TYPE_LAST],
                         ucs_sys_device_t local_sys_dev)
{
    const ucp_worker_iface_t *wiface;
    ucp_rsc_index_t tl_id;
    int tl_type;

    /** TODO: Maybe cache results */
    for (tl_type = UCP_DEVICE_TL_TYPE_FIRST;
         tl_type < UCP_DEVICE_TL_TYPE_LAST; tl_type++) {
        UCS_STATIC_BITMAP_RESET_ALL(&tl_bitmap[tl_type]);
    }

    UCP_TL_BITMAP_FOR_EACH_BIT(tl_id, &worker->context->tl_bitmap) {
        wiface = ucp_worker_iface(worker, tl_id);

        if (!(wiface->attr.cap.flags & UCT_IFACE_FLAG_DEVICE_EP)) {
            continue;
        }

        if ((wiface->attr.ctl_device != UCS_SYS_DEVICE_ID_UNKNOWN) &&
            (wiface->attr.ctl_device != local_sys_dev)) {
            continue;
        }

        if (wiface->attr.cap.flags & UCT_IFACE_FLAG_DEVICE_LKEY) {
            tl_type = UCP_DEVICE_TL_TYPE_LKEY;
        } else {
            tl_type = UCP_DEVICE_TL_TYPE_NOLKEY;
        }
        UCS_STATIC_BITMAP_SET(&tl_bitmap[tl_type], tl_id);
    }
}

/* Keep the host copy of a memory list, the lanes of an element are the
 * resources of the bitmap, in order, repeated up to num_lanes */
static void ucp_device_mem_list_keys_init(ucp_device_mem_list_keys_t *keys,
                                          void *handle, size_t header_size,
                                          size_t elem_size, int is_remote,
                                          unsigned num_lanes,
                                          const ucp_tl_bitmap_t *tl_bitmap)
{
    unsigned i = 0;
    ucp_rsc_index_t tl_id;

    keys->handle    = handle;
    keys->elem_size = elem_size;
    keys->is_remote = is_remote;
    keys->num_lanes = ucs_min(num_lanes, UCP_MAX_LANES);
    while (i < keys->num_lanes) {
        UCP_TL_BITMAP_FOR_EACH_BIT(tl_id, tl_bitmap) {
            if (i < keys->num_lanes) {
                keys->rscs[i++] = tl_id;
            }
        }
    }

    /* The elements follow the header */
    ucs_assert(header_size == (is_remote ?
                               sizeof(ucp_device_remote_mem_list_t) :
                               sizeof(ucp_device_local_mem_list_t)));
}

static ucs_status_t ucp_device_local_mem_list_element_pack(
        const ucp_worker_h worker, const ucp_worker_iface_t *wiface,
        const ucp_device_mem_list_elem_t *element,
        const ucs_memory_type_t mem_type, uct_device_mem_elem_t *mem_element,
        ucp_device_mem_elem_release_handles_t *release_handles)
{
    ucp_tl_resource_desc_t *resource;
    ucp_md_index_t md_index;
    ucp_mem_h memh;
    uct_mem_h uct_memh;
    ucp_tl_md_t *ucp_md;
    void *release_handle;
    ucs_status_t status;

    if (wiface == NULL) {
        return UCS_OK;
    }

    resource = &worker->context->tl_rscs[wiface->rsc_index];
    md_index = resource->md_index;
    ucp_md   = &worker->context->tl_mds[md_index];
    memh = UCS_PARAM_VALUE(UCP_DEVICE_MEM_LIST_ELEM_FIELD, element, memh, MEMH,
                           NULL);
    uct_memh = memh->uct[md_index];
    if (uct_memh == UCT_MEM_HANDLE_NULL) {
        ucs_error("invalid memh for md_index=%u", md_index);
        return UCS_ERR_INVALID_PARAM;
    }

    status = uct_md_mem_elem_pack(ucp_md->md, uct_memh, UCT_INVALID_RKEY,
                                  mem_element, &release_handle);
    if (status != UCS_OK) {
        ucs_error("failed to pack local mem element for memh=%p", memh);
        return status;
    }

    status = ucp_device_mem_elem_release_handles_append(release_handles,
                                                        ucp_md->md,
                                                        release_handle);
    if (status != UCS_OK) {
        uct_md_mem_elem_release(ucp_md->md, release_handle);
    }
    return status;
}

static ucs_status_t ucp_device_mem_list_export_handle(
        ucp_worker_h worker, const void *handle, size_t handle_size,
        ucs_memory_type_t mem_type, ucs_sys_device_t local_sys_dev,
        uct_allocated_memory_t *mem, const char *alloc_name)
{
    ucs_status_t status;

    status = ucp_mem_do_alloc(worker->context, NULL, handle_size,
                              UCT_MD_MEM_ACCESS_LOCAL_READ |
                                      UCT_MD_MEM_ACCESS_LOCAL_WRITE,
                              mem_type, local_sys_dev, alloc_name, mem);
    if (status != UCS_OK) {
        ucs_error("failed to allocate %s mem_type=%s sys_dev=%s: %s",
                  alloc_name, ucs_memory_type_names[mem_type],
                  ucs_topo_sys_device_get_name(local_sys_dev),
                  ucs_status_string(status));
        return status;
    }

    ucp_mem_type_unpack(worker, mem->address, handle, handle_size, mem_type,
                        NULL);
    return UCS_OK;
}

static ucs_status_t ucp_device_local_mem_list_create_handle(
        const ucp_device_mem_list_params_t *params, ucs_memory_type_t mem_type,
        ucs_sys_device_t local_sys_dev, uct_allocated_memory_t *mem,
        ucp_device_mem_elem_release_handles_t *release_handles,
        ucp_device_mem_list_keys_t *keys)
{
    const ucp_worker_h worker   = UCS_PARAM_VALUE(
            UCP_DEVICE_MEM_LIST_PARAMS_FIELD, params, worker, WORKER, NULL);
    size_t uct_elem_size;
    size_t handle_size;
    int tl_type = UCP_DEVICE_TL_TYPE_LKEY;
    ucp_tl_bitmap_t tl_bitmap[UCP_DEVICE_TL_TYPE_LAST] = {};
    const ucp_device_mem_list_elem_t *ucp_element;
    const ucp_worker_iface_t *wiface;
    ucp_device_local_mem_list_t *handle;
    uct_device_local_mem_elem_t *uct_element;
    uct_device_mem_elem_t *tl_element;
    size_t i, num_lanes;
    ucs_status_t status;
    ucp_rsc_index_t tl_id;
    void *local_addr;

    ucp_device_get_tl_bitmap(worker, tl_bitmap, local_sys_dev);
    num_lanes = UCS_STATIC_BITMAP_POPCOUNT(tl_bitmap[tl_type]);

    uct_elem_size = sizeof(uct_device_local_mem_elem_t) +
                    (sizeof(uct_device_mem_elem_t) * num_lanes);
    handle_size   = (uct_elem_size * params->num_elements) + sizeof(*handle);
    handle        = ucs_calloc(1, handle_size, "ucp_device_local_mem_list_t");
    if (handle == NULL) {
        ucs_error("failed to allocate ucp_device_local_mem_list_t");
        return UCS_ERR_NO_MEMORY;
    }

    /* Populate element specific parameters */
    ucp_element = params->elements;
    uct_element = UCS_PTR_TYPE_OFFSET(handle, *handle);
    for (i = 0; i < params->num_elements; i++) {
        local_addr        = UCS_PARAM_VALUE(UCP_DEVICE_MEM_LIST_ELEM_FIELD,
                                            ucp_element, local_addr, LOCAL_ADDR, NULL);
        uct_element->addr = local_addr;
        tl_element        = uct_element->tl;
        UCP_TL_BITMAP_FOR_EACH_BIT(tl_id, &tl_bitmap[tl_type]) {
            wiface = ucp_worker_iface(worker, tl_id);
            status = ucp_device_local_mem_list_element_pack(worker, wiface,
                                                            ucp_element,
                                                            mem_type,
                                                            tl_element,
                                                            release_handles);
            if (status != UCS_OK) {
                ucs_error("failed to pack local mem list element for "
                          "element=%zu",
                          i);
                goto out;
            }

            tl_element = UCS_PTR_TYPE_OFFSET(tl_element, *tl_element);
        }
        uct_element = (void*)tl_element;
        ucp_element = UCS_PTR_BYTE_OFFSET(ucp_element, params->element_size);
    }

    handle->version = UCP_DEVICE_MEM_LIST_VERSION_V1;
    handle->length  = params->num_elements;
    handle->num_lanes = num_lanes;
    status          = ucp_device_mem_list_export_handle(
            worker, handle, handle_size, mem_type, local_sys_dev, mem,
            "ucp_device_local_mem_list_handle_t");
    if (status == UCS_OK) {
        ucp_device_mem_list_keys_init(keys, handle, sizeof(*handle),
                                      uct_elem_size, 0, num_lanes,
                                      &tl_bitmap[tl_type]);
        return UCS_OK;
    }

out:
    ucs_free(handle);
    return status;
}

static ucs_status_t ucp_device_local_mem_list_params_check(
        const ucp_device_mem_list_params_t *params, ucs_memory_type_t mem_type,
        ucs_sys_device_t local_sys_dev)
{
    const ucp_device_mem_list_elem_t *local_elements = UCS_PARAM_VALUE(
            UCP_DEVICE_MEM_LIST_PARAMS_FIELD, params, elements, ELEMENTS, NULL);
    const ucp_worker_h worker                        = UCS_PARAM_VALUE(
            UCP_DEVICE_MEM_LIST_PARAMS_FIELD, params, worker, WORKER, NULL);
    size_t element_size = UCS_PARAM_VALUE(UCP_DEVICE_MEM_LIST_PARAMS_FIELD,
                                          params, element_size, ELEMENT_SIZE,
                                          0);
    size_t num_elements = UCS_PARAM_VALUE(UCP_DEVICE_MEM_LIST_PARAMS_FIELD,
                                          params, num_elements, NUM_ELEMENTS,
                                          0);
    const ucp_device_mem_list_elem_t *element;
    ucp_mem_h memh;
    size_t i;

    if ((local_elements == NULL) || (element_size == 0) ||
        (num_elements == 0) || (worker == NULL)) {
        ucs_error("invalid local mem list params: local_elements=%p, "
                  "element_size=%zu, num_elements=%zu, worker=%p",
                  local_elements, element_size, num_elements, worker);
        return UCS_ERR_INVALID_PARAM;
    }

    for (i = 0; i < num_elements; i++) {
        element = (const ucp_device_mem_list_elem_t*)
                UCS_PTR_BYTE_OFFSET(local_elements, i * element_size);
        memh = UCS_PARAM_VALUE(UCP_DEVICE_MEM_LIST_ELEM_FIELD, element, memh,
                               MEMH, NULL);
        if (memh == NULL) {
            ucs_error("missing memh for element=%zu", i);
            return UCS_ERR_INVALID_PARAM;
        }

        if (memh->sys_dev != local_sys_dev) {
            ucs_error("mismatched local sys_dev for element=%zu", i);
            return UCS_ERR_UNSUPPORTED;
        }
    }

    return UCS_OK;
}

static ucs_status_t
ucp_device_detect_export_mem_type(const ucp_context_h context,
                                  ucs_memory_type_t *export_mem_type_p,
                                  ucs_sys_device_t *sys_dev_p)
{
    ucs_status_t status = UCS_ERR_UNSUPPORTED;
    ucs_memory_type_t mem_type;

    ucs_for_each_bit(mem_type, UCP_DEVICE_MEM_TYPES) {
        status = ucp_device_detect_local_sys_dev(context, mem_type, sys_dev_p);
        if (status == UCS_OK) {
            *export_mem_type_p = mem_type;
            break;
        }
    }

    return status;
}

ucs_status_t
ucp_device_local_mem_list_create(const ucp_device_mem_list_params_t *params,
                                 ucp_device_local_mem_list_h *mem_list_h)
{
    ucp_device_mem_elem_release_handles_t release_handles =
            UCS_ARRAY_DYNAMIC_INITIALIZER;
    const ucp_worker_h worker = UCS_PARAM_VALUE(UCP_DEVICE_MEM_LIST_PARAMS_FIELD,
                                               params, worker, WORKER, NULL);
    ucp_device_mem_list_keys_t keys = {};
    ucs_memory_type_t export_mem_type;
    ucs_sys_device_t local_sys_dev;
    uct_allocated_memory_t mem;
    ucs_status_t status;

    if (worker == NULL) {
        ucs_error("missing worker in local mem list params");
        return UCS_ERR_INVALID_PARAM;
    }

    status = ucp_device_detect_export_mem_type(worker->context,
                                               &export_mem_type,
                                               &local_sys_dev);
    if (status != UCS_OK) {
        return status;
    }

    status = ucp_device_local_mem_list_params_check(params, export_mem_type,
                                                    local_sys_dev);
    if (status != UCS_OK) {
        ucs_error("failed to check local mem list params: %s",
                  ucs_status_string(status));
        return status;
    }

    status = ucp_device_local_mem_list_create_handle(params, export_mem_type,
                                                     local_sys_dev, &mem,
                                                     &release_handles, &keys);
    if (status != UCS_OK) {
        ucs_error("failed to create local mem list handle: %s",
                  ucs_status_string(status));
        goto err;
    }

    /* Track memory allocator for later release */
    status = ucp_device_mem_handle_hash_insert(&mem, params->num_elements,
                                               &release_handles, &keys);
    if (status != UCS_OK) {
        uct_mem_free(&mem);
        goto err;
    }

    *mem_list_h = mem.address;
    return UCS_OK;

err:
    ucp_device_mem_elem_release_handles_cleanup(&release_handles);
    ucs_free(keys.handle);
    return status;
}

static ucp_lane_index_t ucp_device_ep_find_lane(const ucp_ep_h ep, ucp_rsc_index_t tl_id)
{
    ucp_lane_index_t lane;

    for (lane = 0; lane < ucp_ep_num_lanes(ep); ++lane) {
        if (ucp_ep_get_rsc_index(ep, lane) == tl_id) {
            return lane;
        }
    }

    return UCP_NULL_LANE;
}

static int ucp_device_ep_check_lanes(const ucp_device_mem_list_elem_t *elem,
                                     ucp_tl_bitmap_t *tl_bitmap)
{
    ucp_ep_h ep                      = elem->ep;
    const ucp_ep_config_t *ep_config = ucp_ep_config(ep);
    ucp_lane_index_t lane;
    ucp_rsc_index_t tl_id;

    if (UCS_STATIC_BITMAP_POPCOUNT(*tl_bitmap) == 0) {
        return 0;
    }

    UCP_TL_BITMAP_FOR_EACH_BIT(tl_id, tl_bitmap) {
        lane = ucp_device_ep_find_lane(ep, tl_id);
        if (lane == UCP_NULL_LANE) {
            return 0;
        }

        if (!(UCS_BIT(ep_config->key.lanes[lane].dst_md_index) &
              elem->rkey->md_map)) {
            return 0;
        }
    }

    return 1;
}

static ucs_status_t ucp_device_remote_mem_list_element_pack(
        const ucp_device_mem_list_elem_t *element, ucp_rsc_index_t tl_id,
        uct_device_remote_tl_elem_t *mem_element,
        ucp_device_mem_elem_release_handles_t *release_handles)
{
    const ucp_ep_h ep     = element->ep;
    const ucp_rkey_h rkey = element->rkey;
    ucp_ep_config_t *ep_config = ucp_ep_config(ep);
    uct_md_h md;
    uint8_t rkey_index;
    uct_rkey_t uct_rkey;
    uct_ep_h uct_ep;
    uct_device_ep_h device_ep;
    void *release_handle;
    ucs_status_t status;
    ucp_lane_index_t lane;

    lane = ucp_device_ep_find_lane(ep, tl_id);
    if (lane == UCP_NULL_LANE) {
        ucs_error("no lane found for ep=%p", ep);
        return UCS_ERR_NO_DEVICE;
    }

    uct_ep = ucp_ep_get_lane(ep, lane);
    if (ucp_wireup_ep_test(uct_ep)) {
        /*
         * UCP_EP_FLAG_REMOTE_CONNECTED is set before ucp_wireup_eps_progress()
         * installs the real transport endpoints, so the lane can still be a
         * proxy here. Let the caller retry with progress.
         */
        return UCS_ERR_NOT_CONNECTED;
    }

    status = uct_ep_get_device_ep(uct_ep, &device_ep);
    if (status != UCS_OK) {
        ucs_error("failed to get device_ep for lane=%u", lane);
        return status;
    }

    rkey_index = ucs_bitmap2idx(rkey->md_map,
                                ep_config->key.lanes[lane].dst_md_index);
    uct_rkey   = ucp_rkey_get_tl_rkey(rkey, rkey_index);
    ucs_assert(uct_rkey != UCT_INVALID_RKEY);

    md              = ucp_ep_md(ep, lane);
    mem_element->ep = device_ep;
    status = uct_md_mem_elem_pack(md, NULL, uct_rkey, &mem_element->uct,
                                  &release_handle);
    if (status != UCS_OK) {
        ucs_error("failed to pack uct memory element for lane=%u", lane);
        return status;
    }

    status = ucp_device_mem_elem_release_handles_append(release_handles, md,
                                                        release_handle);
    if (status != UCS_OK) {
        uct_md_mem_elem_release(md, release_handle);
    }

    return status;
}

#define UCP_DEVICE_MEM_LIST_GET_ELEMENT(_params, _i) \
    ((ucp_device_mem_list_elem_t*)UCS_PTR_BYTE_OFFSET( \
            (_params)->elements, (_i) * (_params)->element_size))
#define UCP_DEVICE_MEM_LIST_GET_EP(_params, _i) \
    (UCS_PARAM_VALUE(UCP_DEVICE_MEM_LIST_ELEM_FIELD, \
                     UCP_DEVICE_MEM_LIST_GET_ELEMENT(_params, _i), ep, EP, \
                     NULL))
#define UCP_DEVICE_MEM_LIST_GET_RKEY(_params, _i) \
    (UCS_PARAM_VALUE(UCP_DEVICE_MEM_LIST_ELEM_FIELD, \
                     UCP_DEVICE_MEM_LIST_GET_ELEMENT(_params, _i), rkey, RKEY, \
                     NULL))
#define UCP_DEVICE_MEM_ELEMENT_IS_GAP(_ucp_element) \
    ((_ucp_element)->field_mask == 0)

static ucp_ep_h ucp_device_remote_mem_list_get_first_ep(
        const ucp_device_mem_list_params_t *params)
{
    const ucp_device_mem_list_elem_t *ucp_element = params->elements;
    size_t i                                      = 0;

    for (i = 0; i < params->num_elements; i++) {
        if (UCP_DEVICE_MEM_ELEMENT_IS_GAP(ucp_element)) {
            ucp_element = UCS_PTR_BYTE_OFFSET(ucp_element,
                                              params->element_size);
            continue;
        }

        return UCS_PARAM_VALUE(UCP_DEVICE_MEM_LIST_ELEM_FIELD, ucp_element, ep,
                               EP, NULL);
    }

    return NULL;
}

static ucs_status_t ucp_device_remote_mem_list_fill(
        const ucp_device_mem_list_elem_t *ucp_element,
        const ucp_tl_bitmap_t *tl_bitmap, size_t num_lanes,
        uct_device_remote_mem_elem_t *uct_element,
        ucp_device_mem_elem_release_handles_t *release_handles)
{
    uct_device_remote_tl_elem_t *tl_element;
    ucp_rsc_index_t tl_id;
    ucs_status_t status;
    size_t i;

    uct_element->addr = ucp_element->remote_addr;
    tl_element        = uct_element->tl;
    for (i = 0; i < num_lanes;) {
        UCP_TL_BITMAP_FOR_EACH_BIT(tl_id, tl_bitmap) {
            status = ucp_device_remote_mem_list_element_pack(ucp_element, tl_id,
                                                             tl_element,
                                                             release_handles);
            if (status != UCS_OK) {
                return status;
            }

            tl_element = UCS_PTR_TYPE_OFFSET(tl_element, *tl_element);
            i++;
        }
    }

    return UCS_OK;
}

static ucs_status_t ucp_device_remote_mem_list_create_handle(
        const ucp_device_mem_list_params_t *params, ucp_ep_h ep,
        ucs_memory_type_t mem_type, ucs_sys_device_t local_sys_dev,
        uct_allocated_memory_t *mem,
        ucp_device_mem_elem_release_handles_t *release_handles,
        ucp_device_mem_list_keys_t *keys)
{
    size_t uct_elem_size;
    size_t handle_size         = 0;
    ucp_tl_bitmap_t tl_bitmap[UCP_DEVICE_TL_TYPE_LAST] = {};
    const ucp_device_mem_list_elem_t *ucp_element;
    ucp_device_remote_mem_list_t *handle;
    uct_device_remote_mem_elem_t *uct_element;
    size_t i, num_lanes;
    ucs_status_t status;
    int tl_type;

    ucp_device_get_tl_bitmap(ep->worker, tl_bitmap, local_sys_dev);

    /* handle->num_lanes is the least common multiple of both lane types, so:
     * - each lane is replicated num_lanes / popcount(tl_bitmap) times
     * - channel_id % num_lanes maps to the correct lane, regardless of lane type
     */
    num_lanes = UCS_STATIC_BITMAP_POPCOUNT(tl_bitmap[UCP_DEVICE_TL_TYPE_LKEY]);
    if (!num_lanes) {
        if (!UCS_STATIC_BITMAP_POPCOUNT(tl_bitmap[UCP_DEVICE_TL_TYPE_NOLKEY])) {
            ucs_error("failed to pack uct memory element for first element");
            return UCS_ERR_INVALID_PARAM;
        }

        ucs_assert(UCS_STATIC_BITMAP_POPCOUNT(
                           tl_bitmap[UCP_DEVICE_TL_TYPE_NOLKEY]) == 1);
        num_lanes = 1;
    }

    ucp_element   = params->elements;
    uct_elem_size = sizeof(uct_device_remote_mem_elem_t) +
                    (sizeof(uct_device_remote_tl_elem_t) * num_lanes);
    handle_size   = sizeof(*handle) + (params->num_elements * uct_elem_size);
    handle        = ucs_calloc(1, handle_size, "ucp_device_remote_mem_list_t");
    if (handle == NULL) {
        ucs_error("failed to allocate ucp_device_remote_mem_list_t");
        return UCS_ERR_NO_MEMORY;
    }

    uct_element = UCS_PTR_TYPE_OFFSET(handle, *handle);
    for (i = 0; i < params->num_elements; i++) {
        if (!UCP_DEVICE_MEM_ELEMENT_IS_GAP(ucp_element)) {
            for (tl_type = UCP_DEVICE_TL_TYPE_FIRST;
                 tl_type < UCP_DEVICE_TL_TYPE_LAST; tl_type++) {
                if (ucp_device_ep_check_lanes(ucp_element,
                                              &tl_bitmap[tl_type])) {
                    break;
                }
            }

            if (tl_type == UCP_DEVICE_TL_TYPE_LAST) {
                ucs_error("lane not found for element %zd", i);
                status = UCS_ERR_INVALID_PARAM;
                goto out;
            }

            status = ucp_device_remote_mem_list_fill(ucp_element,
                                                     &tl_bitmap[tl_type],
                                                     num_lanes, uct_element,
                                                     release_handles);
            if (status != UCS_OK) {
                goto out;
            }
        }
        uct_element = UCS_PTR_BYTE_OFFSET(uct_element, uct_elem_size);
        ucp_element = UCS_PTR_BYTE_OFFSET(ucp_element, params->element_size);
    }

    handle->version   = UCP_DEVICE_MEM_LIST_VERSION_V1;
    handle->length    = params->num_elements;
    handle->num_lanes = num_lanes;
    status            = ucp_device_mem_list_export_handle(
            ep->worker, handle, handle_size, mem_type, local_sys_dev, mem,
            "ucp_device_remote_mem_list_handle_t");
    if ((status == UCS_OK) &&
        UCS_STATIC_BITMAP_POPCOUNT(tl_bitmap[UCP_DEVICE_TL_TYPE_LKEY])) {
        ucp_device_mem_list_keys_init(keys, handle, sizeof(*handle),
                                      uct_elem_size, 1, num_lanes,
                                      &tl_bitmap[UCP_DEVICE_TL_TYPE_LKEY]);
        return UCS_OK;
    }

out:
    ucs_free(handle);
    return status;
}

static ucs_status_t ucp_device_remote_mem_list_params_check(
        const ucp_device_mem_list_params_t *params)
{
    const ucp_device_mem_list_elem_t *remote_elements = UCS_PARAM_VALUE(
            UCP_DEVICE_MEM_LIST_PARAMS_FIELD, params, elements, ELEMENTS, NULL);
    size_t element_size = UCS_PARAM_VALUE(UCP_DEVICE_MEM_LIST_PARAMS_FIELD,
                                          params, element_size, ELEMENT_SIZE,
                                          0);
    size_t num_elements = UCS_PARAM_VALUE(UCP_DEVICE_MEM_LIST_PARAMS_FIELD,
                                          params, num_elements, NUM_ELEMENTS,
                                          0);
    const ucp_device_mem_list_elem_t *remote_element;
    ucp_rkey_h rkey;
    ucp_ep_h ep;
    size_t i;

    if (remote_elements == NULL || element_size == 0 || num_elements == 0) {
        ucs_error("invalid remote mem list params: remote_elements=%p, "
                  "element_size=%zu, num_elements=%zu",
                  remote_elements, element_size, num_elements);
        return UCS_ERR_INVALID_PARAM;
    }

    for (i = 0; i < num_elements; i++) {
        remote_element = UCP_DEVICE_MEM_LIST_GET_ELEMENT(params, i);
        if (UCP_DEVICE_MEM_ELEMENT_IS_GAP(remote_element)) {
            continue;
        }

        rkey = UCS_PARAM_VALUE(UCP_DEVICE_MEM_LIST_ELEM_FIELD, remote_element,
                               rkey, RKEY, NULL);
        ep = UCS_PARAM_VALUE(UCP_DEVICE_MEM_LIST_ELEM_FIELD, remote_element, ep,
                             EP, NULL);
        if ((rkey == NULL) || (ep == NULL)) {
            return UCS_ERR_INVALID_PARAM;
        }

        if (!(ep->flags & UCP_EP_FLAG_REMOTE_CONNECTED) &&
            (ucp_ep_config(ep)->p2p_lanes != 0)) {
            /*
             * Do not log error here because UCS_ERR_NOT_CONNECTED is expected
             * during connection establishment. Applications are expected to retry
             * with progress.
             */
            return UCS_ERR_NOT_CONNECTED;
        }
    }

    return UCS_OK;
}


ucs_status_t
ucp_device_remote_mem_list_create(const ucp_device_mem_list_params_t *params,
                                  ucp_device_remote_mem_list_h *mem_list_h)
{
    ucp_ep_h ep = ucp_device_remote_mem_list_get_first_ep(params);
    ucp_device_mem_elem_release_handles_t release_handles =
            UCS_ARRAY_DYNAMIC_INITIALIZER;
    ucp_device_mem_list_keys_t keys = {};
    ucs_memory_type_t export_mem_type;
    ucs_sys_device_t sys_dev;
    uct_allocated_memory_t mem;
    ucs_status_t status;

    if (ep == NULL) {
        ucs_error("no ep found in remote mem list params");
        return UCS_ERR_INVALID_PARAM;
    }

    status = ucp_device_detect_export_mem_type(ep->worker->context,
                                               &export_mem_type, &sys_dev);
    if (status != UCS_OK) {
        return status;
    }

    status = ucp_device_remote_mem_list_params_check(params);
    if (status != UCS_OK) {
        return status;
    }

    status = ucp_device_remote_mem_list_create_handle(params, ep,
                                                      export_mem_type, sys_dev,
                                                      &mem, &release_handles,
                                                      &keys);
    if (status != UCS_OK) {
        /*
         * Do not log error for UCS_ERR_NOT_CONNECTED because it is expected
         * during connection establishment. Applications are expected to retry
         * with progress.
         */
        if (status != UCS_ERR_NOT_CONNECTED) {
            ucs_error("failed to create handle: %s", ucs_status_string(status));
        }
        goto err;
    }

    /* Track memory allocator for later release */
    status = ucp_device_mem_handle_hash_insert(&mem, params->num_elements,
                                               &release_handles, &keys);
    if (status != UCS_OK) {
        uct_mem_free(&mem);
        goto err;
    }

    *mem_list_h = mem.address;
    return UCS_OK;

err:
    ucp_device_mem_elem_release_handles_cleanup(&release_handles);
    ucs_free(keys.handle);
    return status;
}

uint32_t ucp_device_get_mem_list_length(const void *mem_list_h)
{
    khiter_t iter;
    uint32_t length;

    ucs_assert(mem_list_h != NULL);

    ucs_spin_lock(&ucp_device_handle_hash_lock);
    iter = kh_get(ucp_device_handle_allocs, &ucp_device_handle_hash,
                  (void*)mem_list_h);
    ucs_assertv_always((iter != kh_end(&ucp_device_handle_hash)), "handle=%p",
                       mem_list_h);
    length = kh_value(&ucp_device_handle_hash, iter).mem_list_length;
    ucs_spin_unlock(&ucp_device_handle_hash_lock);

    return length;
}

void ucp_device_mem_list_release(void *mem_list_h)
{
    ucp_device_handle_info_t info;

    info = ucp_device_mem_handle_hash_remove(mem_list_h);
    ucp_device_mem_elem_release_handles_cleanup(&info.release_handles);
    ucs_free(info.keys.handle);
    uct_mem_free(&info.mem);
}

/*
 * Resolve the memory info of the counter area. Returns NULL when the memory
 * flags cannot be resolved, in which case only @a mem_info->type is set and
 * the memory type endpoint looks the flags up by itself.
 */
static const ucp_memory_info_t *
ucp_device_counter_mem_info(ucp_context_h context, const void *counter_ptr,
                            const ucp_device_counter_params_t *params,
                            ucp_memory_info_t *mem_info)
{
    if (params->field_mask & UCP_DEVICE_COUNTER_PARAMS_FIELD_MEMH) {
        *mem_info = ucp_memory_info_from_memh(params->memh);
        return mem_info;
    }

    if (params->field_mask & UCP_DEVICE_COUNTER_PARAMS_FIELD_MEM_TYPE) {
        /* The memory flags are not part of the caller's hint */
        mem_info->type = params->mem_type;
        return NULL;
    }

    ucp_memory_detect(context, counter_ptr, sizeof(uint64_t), mem_info);
    return mem_info;
}

ucs_status_t ucp_device_counter_init(ucp_worker_h worker,
                                     const ucp_device_counter_params_t *params,
                                     void *counter_ptr)
{
    uint64_t counter_value = 0;
    ucp_memory_info_t mem_info;
    const ucp_memory_info_t *resolved;

    resolved = ucp_device_counter_mem_info(worker->context, counter_ptr, params,
                                           &mem_info);
    ucp_dt_contig_unpack(worker, counter_ptr, &counter_value,
                         sizeof(counter_value),
                         (ucs_memory_type_t)mem_info.type, resolved,
                         sizeof(counter_value));
    return UCS_OK;
}

uint64_t ucp_device_counter_read(ucp_worker_h worker,
                                 const ucp_device_counter_params_t *params,
                                 void *counter_ptr)
{
    uint64_t counter_value = 0;
    ucp_memory_info_t mem_info;
    const ucp_memory_info_t *resolved;

    resolved = ucp_device_counter_mem_info(worker->context, counter_ptr, params,
                                           &mem_info);
    ucp_dt_contig_pack(worker, &counter_value, counter_ptr,
                       sizeof(counter_value), (ucs_memory_type_t)mem_info.type,
                       resolved, sizeof(counter_value));
    return counter_value;
}

/* Size of the buffers for the RX tokens of a device lane */
#define UCP_DEVICE_LANE_TOKEN_MAX 1024

/* How often a peer sends its RX token again, until it gets the token of the
 * other peer, since a message on a failed lane may be lost */
#define UCP_DEVICE_LANE_TOKEN_RESEND_SEC 1.0


/*
 * Failover of a failed device lane. Each peer pauses its transport endpoint
 * and sends its RX token to the other one. With both, a peer moves what the
 * other one did not receive to another device lane. A peer which receives a
 * token before it noticed the failure joins the round. A peer without a lane
 * to move to fails the lane, and sends an empty token, so the other one does
 * not wait for its token. Rounds are counted by both peers alike, to drop late
 * messages.
 */
typedef struct ucp_device_lane_reset {
    ucp_ep_h         ep;
    ucp_lane_index_t lane;
    uint32_t         round;      /* Current round, or the last one */
    uint8_t          active;     /* The round is in progress */
    uint8_t          scheduled;  /* Progress is scheduled */
    uint8_t          paused;     /* The endpoint is paused, token is set */
    uint8_t          sent;       /* The token was sent to the peer */
    uint8_t          has_peer;   /* The peer token is set */
    uint8_t          failed;     /* Device operations on the lane fail */
    ucp_lane_index_t to_lane;    /* Lane to move to, chosen once per round */
    ucs_time_t       sent_time;  /* When the token was sent last */
    size_t           token_length;
    size_t           peer_token_length;
    uint8_t          token[UCP_DEVICE_LANE_TOKEN_MAX];
    uint8_t          peer_token[UCP_DEVICE_LANE_TOKEN_MAX];
} ucp_device_lane_reset_t;


struct ucp_device_lane_resets {
    ucp_device_lane_reset_t *lanes[UCP_MAX_LANES];
};


static ucp_device_lane_reset_t *
ucp_device_lane_reset_get(ucp_ep_h ep, ucp_lane_index_t lane)
{
    ucp_device_lane_reset_t *reset;

    if (ep->ext->device_lane_resets == NULL) {
        ep->ext->device_lane_resets = ucs_calloc(
                1, sizeof(*ep->ext->device_lane_resets),
                "ucp_device_lane_resets");
        if (ep->ext->device_lane_resets == NULL) {
            return NULL;
        }
    }

    reset = ep->ext->device_lane_resets->lanes[lane];
    if (reset == NULL) {
        reset = ucs_calloc(1, sizeof(*reset), "ucp_device_lane_reset");
        if (reset == NULL) {
            return NULL;
        }

        reset->ep                                 = ep;
        reset->lane                               = lane;
        ep->ext->device_lane_resets->lanes[lane] = reset;
    }

    return reset;
}

void ucp_ep_device_lane_resets_free(ucp_ep_h ep)
{
    ucp_lane_index_t lane;

    if (ep->ext->device_lane_resets == NULL) {
        return;
    }

    for (lane = 0; lane < UCP_MAX_LANES; ++lane) {
        /* Progress holds a reference to the endpoint */
        ucs_assert((ep->ext->device_lane_resets->lanes[lane] == NULL) ||
                   !ep->ext->device_lane_resets->lanes[lane]->scheduled);
        ucs_free(ep->ext->device_lane_resets->lanes[lane]);
    }

    ucs_free(ep->ext->device_lane_resets);
    ep->ext->device_lane_resets = NULL;
}

/* Lanes to translate the memory keys between, see ucp_device_failover_key() */
typedef struct {
    ucp_rsc_index_t from_rsc;
    ucp_rsc_index_t to_rsc;
} ucp_device_failover_key_arg_t;


static int ucp_device_mem_list_keys_lane(const ucp_device_mem_list_keys_t *keys,
                                         ucp_rsc_index_t rsc)
{
    unsigned i;

    for (i = 0; i < keys->num_lanes; ++i) {
        if (keys->rscs[i] == rsc) {
            return i;
        }
    }

    return -1;
}

/* Find the key in the memory lists, and return the key of the same element
 * for the other lane */
static ucs_status_t ucp_device_failover_key(void *arg, int is_remote,
                                            uint32_t key, uint32_t *key_p)
{
    const ucp_device_failover_key_arg_t *key_arg = arg;
    ucs_status_t status                         = UCS_ERR_NO_ELEM;
    const uct_device_remote_mem_elem_t *remote_elem;
    const uct_device_local_mem_elem_t *local_elem;
    const ucp_device_mem_list_keys_t *keys;
    ucp_device_handle_info_t *info;
    int from, to;
    uint32_t i, length;
    const void *elem;
    khiter_t iter;

    ucs_spin_lock(&ucp_device_handle_hash_lock);
    for (iter = kh_begin(&ucp_device_handle_hash);
         iter != kh_end(&ucp_device_handle_hash); ++iter) {
        if (!kh_exist(&ucp_device_handle_hash, iter)) {
            continue;
        }

        info = &kh_value(&ucp_device_handle_hash, iter);
        keys = &info->keys;
        if ((keys->handle == NULL) || (keys->is_remote != is_remote)) {
            continue;
        }

        from = ucp_device_mem_list_keys_lane(keys, key_arg->from_rsc);
        to   = ucp_device_mem_list_keys_lane(keys, key_arg->to_rsc);
        if ((from < 0) || (to < 0)) {
            continue;
        }

        length = info->mem_list_length;
        elem   = UCS_PTR_BYTE_OFFSET(keys->handle,
                                     is_remote ?
                                     sizeof(ucp_device_remote_mem_list_t) :
                                     sizeof(ucp_device_local_mem_list_t));
        for (i = 0; i < length;
             ++i, elem = UCS_PTR_BYTE_OFFSET(elem, keys->elem_size)) {
            if (is_remote) {
                remote_elem = elem;
                if ((remote_elem->tl[from].ep == NULL) ||
                    (remote_elem->tl[from].uct.ib_md_mem_element.rkey != key)) {
                    continue;
                }

                *key_p = remote_elem->tl[to].uct.ib_md_mem_element.rkey;
            } else {
                local_elem = elem;
                if (local_elem->tl[from].ib_md_mem_element.lkey != key) {
                    continue;
                }

                *key_p = local_elem->tl[to].ib_md_mem_element.lkey;
            }

            status = UCS_OK;
            goto out;
        }
    }

out:
    ucs_spin_unlock(&ucp_device_handle_hash_lock);
    if (status != UCS_OK) {
        ucs_error("no %s key 0x%x of rsc %u in device memory lists",
                  is_remote ? "remote" : "local", key, key_arg->from_rsc);
    }

    return status;
}

/*
 * The lane a failed device lane moves to: the next one, in the order of the
 * lanes of the memory lists, which did not fail, as in
 * ucp_device_select_lane()
 */
static ucp_lane_index_t
ucp_device_failover_target_lane(ucp_ep_h ep, ucp_lane_index_t failed_lane)
{
    ucp_rsc_index_t failed_rsc = ucp_ep_get_rsc_index(ep, failed_lane);
    ucp_lane_index_t target    = UCP_NULL_LANE;
    ucp_lane_index_t lane;
    ucp_rsc_index_t rsc, best_rsc, wrap_rsc;
    ucp_lane_index_t wrap      = UCP_NULL_LANE;

    best_rsc = UCP_NULL_RESOURCE;
    wrap_rsc = UCP_NULL_RESOURCE;
    for (lane = 0; lane < ucp_ep_num_lanes(ep); ++lane) {
        if ((lane == failed_lane) ||
            !(ucp_ep_config(ep)->key.lanes[lane].lane_types &
              UCS_BIT(UCP_LANE_TYPE_DEVICE)) ||
            (ep->ext->failed_device_lanes & UCS_BIT(lane))) {
            continue;
        }

        rsc = ucp_ep_get_rsc_index(ep, lane);
        if (strcmp(ep->worker->context->tl_rscs[rsc].tl_rsc.tl_name,
                   ep->worker->context->tl_rscs[failed_rsc].tl_rsc.tl_name)) {
            continue;
        }

        if ((rsc > failed_rsc) &&
            ((best_rsc == UCP_NULL_RESOURCE) || (rsc < best_rsc))) {
            best_rsc = rsc;
            target   = lane;
        } else if ((rsc < failed_rsc) &&
                   ((wrap_rsc == UCP_NULL_RESOURCE) || (rsc < wrap_rsc))) {
            wrap_rsc = rsc;
            wrap     = lane;
        }
    }

    return (target != UCP_NULL_LANE) ? target : wrap;
}

static unsigned ucp_device_lane_reset_progress(void *arg);

static void ucp_device_lane_reset_schedule(ucp_device_lane_reset_t *reset)
{
    ucp_worker_h worker = reset->ep->worker;

    if (reset->scheduled) {
        return;
    }

    reset->scheduled = 1;
    ucp_ep_refcount_add(reset->ep, device_reset);
    ucs_callbackq_add_oneshot(&worker->uct->progress_q, reset->ep,
                              ucp_device_lane_reset_progress, reset);
    ucp_worker_signal_internal(worker);
}

static void ucp_device_lane_reset_start(ucp_device_lane_reset_t *reset,
                                        uint32_t round)
{
    reset->round        = round;
    reset->active       = 1;
    reset->paused       = 0;
    reset->sent         = 0;
    reset->has_peer     = 0;
    reset->token_length = 0;
    reset->to_lane      = UCP_NULL_LANE;
    ucp_device_lane_reset_schedule(reset);
}

/* Make the device operations on a lane which cannot fail over fail */
static void ucp_device_lane_fail(ucp_device_lane_reset_t *reset,
                                 const char *reason, ucs_status_t status)
{
    ucp_ep_h ep           = reset->ep;
    ucp_lane_index_t lane = reset->lane;
    UCS_STRING_BUFFER_ONSTACK(strb, 64);
    ucs_status_t fail_status;

    reset->active                 = 0;
    reset->failed                 = 1;
    ep->ext->failed_device_lanes |= UCS_BIT(lane);

    ucs_error(UCP_EP_LANE_FMT ": device lane cannot fail over in round %u, %s:"
              " %s", UCP_EP_LANE_ARG(ep, lane, &strb), reset->round, reason,
              ucs_status_string(status));

    fail_status = uct_ep_failover_fail(ucp_ep_get_lane(ep, lane));
    if (fail_status != UCS_OK) {
        ucs_error(UCP_EP_LANE_FMT ": failed to fail device lane: %s",
                  UCP_EP_LANE_ARG(ep, lane, &strb),
                  ucs_status_string(fail_status));
    }
}

/* An empty token tells the peer that this side failed the lane */
static void
ucp_device_lane_send_failed(ucp_device_lane_reset_t *reset, uint32_t round)
{
    (void)ucp_wireup_send_device_lane_reset(reset->ep, reset->lane, round,
                                            reset->token, 0);
}

/* Fail a lane without a device lane to move to, and tell the peer */
static void
ucp_device_lane_fail_no_target(ucp_device_lane_reset_t *reset, uint32_t round)
{
    reset->round = round;
    ucp_device_lane_fail(reset, "no device lane to fail over to",
                         UCS_ERR_UNREACHABLE);
    ucp_device_lane_send_failed(reset, round);
}

static ucs_status_t ucp_device_lane_reset_send(ucp_device_lane_reset_t *reset)
{
    ucs_status_t status;

    status = ucp_wireup_send_device_lane_reset(reset->ep, reset->lane,
                                               reset->round, reset->token,
                                               reset->token_length);
    if (status != UCS_OK) {
        return status;
    }

    reset->sent      = 1;
    reset->sent_time = ucs_get_time();
    return UCS_OK;
}

static unsigned ucp_device_lane_reset_progress(void *arg)
{
    ucp_device_lane_reset_t *reset = arg;
    ucp_ep_h ep                    = reset->ep;
    ucp_lane_index_t lane          = reset->lane;
    UCS_STRING_BUFFER_ONSTACK(strb, 64);
    ucp_device_failover_key_arg_t key_arg;
    ucp_lane_index_t to_lane;
    uct_ep_h uct_ep;
    ucs_status_t status;
    size_t length;

    UCS_ASYNC_BLOCK(&ep->worker->async);

    reset->scheduled = 0;
    if (ep->flags & (UCP_EP_FLAG_FAILED | UCP_EP_FLAG_CLOSED)) {
        reset->active = 0;
        goto out;
    }

    uct_ep = ucp_ep_get_lane(ep, lane);
    if (!reset->paused) {
        length = sizeof(reset->token);
        status = uct_ep_failover_pause(uct_ep, reset->token, &length);
        if (status == UCS_ERR_NO_RESOURCE) {
            /* The device did not finish posting yet */
            ucp_device_lane_reset_schedule(reset);
            goto out;
        } else if (status != UCS_OK) {
            ucp_device_lane_fail(reset, "pause failed", status);
            ucp_device_lane_send_failed(reset, reset->round);
            goto out;
        }

        reset->paused       = 1;
        reset->token_length = length;
    }

    if (!reset->sent ||
        (!reset->has_peer &&
         (ucs_get_time() - reset->sent_time) >
                 ucs_time_from_sec(UCP_DEVICE_LANE_TOKEN_RESEND_SEC))) {
        status = ucp_device_lane_reset_send(reset);
        if (status != UCS_OK) {
            ucp_device_lane_fail(reset, "sending the RX token failed",
                                 status);
            goto out;
        }
    }

    if (!reset->has_peer) {
        /* Continued by the token of the peer, send it again meanwhile */
        ucp_device_lane_reset_schedule(reset);
        goto out;
    }

    /* Fail over to another device lane, which device operations of the
     * failed lane use from now on. Another lane may fail meanwhile. */
    if (reset->to_lane == UCP_NULL_LANE) {
        reset->to_lane = ucp_device_failover_target_lane(ep, lane);
        if (reset->to_lane == UCP_NULL_LANE) {
            ucp_device_lane_fail_no_target(reset, reset->round);
            goto out;
        }
    }

    to_lane = reset->to_lane;

    key_arg.from_rsc = ucp_ep_get_rsc_index(ep, lane);
    key_arg.to_rsc   = ucp_ep_get_rsc_index(ep, to_lane);
    status           = uct_ep_failover_move(uct_ep,
                                            ucp_ep_get_lane(ep, to_lane),
                                            reset->peer_token,
                                            reset->peer_token_length,
                                            ucp_device_failover_key, &key_arg);
    if (status == UCS_ERR_NO_RESOURCE) {
        ucp_device_lane_reset_schedule(reset);
        goto out;
    } else if (status != UCS_OK) {
        ucp_device_lane_fail(reset, "moving to another lane failed", status);
        ucp_device_lane_send_failed(reset, reset->round);
        goto out;
    }

    ucs_diag(UCP_EP_LANE_FMT ": device lane failed over to lane[%d] in round"
             " %u", UCP_EP_LANE_ARG(ep, lane, &strb), to_lane, reset->round);

    /* The lane stays failed until it recovers */
    reset->active = 0;

out:
    UCS_ASYNC_UNBLOCK(&ep->worker->async);
    ucp_ep_refcount_remove(ep, device_reset); /* may destroy the EP */
    return 1;
}

void ucp_ep_device_lane_failed(ucp_ep_h ep, ucp_lane_index_t lane,
                               ucs_status_t status)
{
    UCS_STRING_BUFFER_ONSTACK(strb, 64);
    ucp_device_lane_reset_t *reset;

    if (ep->ext->failed_device_lanes & UCS_BIT(lane)) {
        return;
    }

    ep->ext->failed_device_lanes |= UCS_BIT(lane);
    /* Only a lane whose transport retries ran out can fail over, see
     * UCT_EP_PARAM_FLAG_FAILOVER */
    if (!ucp_ep_err_mode_eq(ep, UCP_ERR_HANDLING_MODE_FAILOVER) ||
        (status != UCS_ERR_ENDPOINT_TIMEOUT)) {
        /* Device operations on this lane fail on the device */
        ucs_diag(UCP_EP_LANE_FMT ": device lane failed: %s, not handled",
                 UCP_EP_LANE_ARG(ep, lane, &strb), ucs_status_string(status));
        return;
    }

    reset = ucp_device_lane_reset_get(ep, lane);
    if (reset == NULL) {
        ucs_error(UCP_EP_LANE_FMT ": failed to allocate device lane reset",
                  UCP_EP_LANE_ARG(ep, lane, &strb));
        return;
    }

    if (reset->active || reset->failed) {
        /* The peer started the round already, or the lane failed */
        return;
    }

    if (ucp_device_failover_target_lane(ep, lane) == UCP_NULL_LANE) {
        ucp_device_lane_fail_no_target(reset, reset->round + 1);
        return;
    }

    ucs_diag(UCP_EP_LANE_FMT ": device lane failed: %s, resetting it in round"
             " %u", UCP_EP_LANE_ARG(ep, lane, &strb), ucs_status_string(status),
             reset->round + 1);
    ucp_device_lane_reset_start(reset, reset->round + 1);
}

void ucp_ep_device_lane_reset_received(ucp_ep_h ep, ucp_lane_index_t lane,
                                       uint32_t round, const void *token,
                                       size_t token_length)
{
    UCS_STRING_BUFFER_ONSTACK(strb, 64);
    ucp_device_lane_reset_t *reset;

    if ((lane >= ucp_ep_num_lanes(ep)) ||
        !(ucp_ep_config(ep)->key.lanes[lane].lane_types &
          UCS_BIT(UCP_LANE_TYPE_DEVICE)) ||
        (token_length > UCP_DEVICE_LANE_TOKEN_MAX)) {
        ucs_error("ep %p: invalid device lane reset message, lane %u token"
                  " length %zu", ep, lane, token_length);
        return;
    }

    reset = ucp_device_lane_reset_get(ep, lane);
    if (reset == NULL) {
        ucs_error(UCP_EP_LANE_FMT ": failed to allocate device lane reset",
                  UCP_EP_LANE_ARG(ep, lane, &strb));
        return;
    }

    if (reset->failed) {
        if (token_length != 0) {
            /* The peer did not get the message that the lane failed */
            ucp_device_lane_send_failed(reset, round);
        }
        return;
    }

    if (token_length == 0) {
        /* The peer failed the lane. Wait for its token no more, but finish
         * moving to another lane once started, which the peer receives. */
        if ((!reset->active && (round == (reset->round + 1))) ||
            (reset->active && (round == reset->round) && !reset->has_peer)) {
            reset->round = round;
            ucp_device_lane_fail(reset, "the peer failed it",
                                 UCS_ERR_CONNECTION_RESET);
        }
        return;
    }

    if (!reset->active && (round == (reset->round + 1))) {
        /* The peer noticed the failure first */
        if (ucp_device_failover_target_lane(ep, lane) == UCP_NULL_LANE) {
            ucp_device_lane_fail_no_target(reset, round);
            return;
        }

        ucs_diag(UCP_EP_LANE_FMT ": peer resets device lane in round %u",
                 UCP_EP_LANE_ARG(ep, lane, &strb), round);
        ep->ext->failed_device_lanes |= UCS_BIT(lane);
        ucp_device_lane_reset_start(reset, round);
    } else if (round != reset->round) {
        ucs_debug(UCP_EP_LANE_FMT ": drop device lane reset of round %u, "
                  "current round %u",
                  UCP_EP_LANE_ARG(ep, lane, &strb), round, reset->round);
        return;
    } else if (!reset->active || reset->has_peer) {
        /* The peer sends its token again, so it did not get ours */
        if (reset->sent) {
            (void)ucp_device_lane_reset_send(reset);
        }
        return;
    }

    memcpy(reset->peer_token, token, token_length);
    reset->peer_token_length = token_length;
    reset->has_peer          = 1;
    if (reset->sent) {
        ucp_device_lane_reset_schedule(reset);
    }
}
