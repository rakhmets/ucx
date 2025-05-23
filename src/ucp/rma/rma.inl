/**
 * Copyright (c) NVIDIA CORPORATION & AFFILIATES, 2001-2018. ALL RIGHTS RESERVED.
 *
 * See file LICENSE for terms.
 */

#ifndef UCP_RMA_INL_
#define UCP_RMA_INL_

#include "rma.h"

#include <ucp/api/ucp.h>
#include <ucp/core/ucp_request.inl>
#include <ucs/debug/log.h>


/* TODO: remove it after AMO API is implemented via NBX  */
static UCS_F_ALWAYS_INLINE ucs_status_ptr_t
ucp_rma_send_request_cb(ucp_request_t *req, ucp_send_callback_t cb)
{
    ucs_status_t status;

    ucp_request_send(req);
    status = req->status;

    if (req->flags & UCP_REQUEST_FLAG_COMPLETED) {
        ucs_trace_req("releasing send request %p, returning status %s", req,
                      ucs_status_string(status));
        ucp_request_put(req);
        return UCS_STATUS_PTR(status);
    }

    ucs_trace_req("returning request %p, status %s", req,
                  ucs_status_string(status));
    ucp_request_set_user_callback(req, send.cb, (ucp_send_nbx_callback_t)cb, NULL);
    return req + 1;
}

static UCS_F_ALWAYS_INLINE ucs_status_ptr_t
ucp_rma_send_request(ucp_request_t *req, const ucp_request_param_t *param)
{
    ucp_request_send(req);

    if (req->flags & UCP_REQUEST_FLAG_COMPLETED) {
        /* Coverity wrongly resolves completion callback function to
         * 'ucp_cm_client_connect_progress' */
        /* coverity[offset_free] */
        ucp_request_imm_cmpl_param(param, req, send);
    }

    ucs_trace_req("returning request %p, status %s", req,
                  ucs_status_string(req->status));

    ucp_request_set_send_callback_param(param, req, send);

    return req + 1;
}

static inline ucs_status_t ucp_rma_wait(ucp_worker_h worker, void *user_req,
                                        const char *op_name)
{
    ucs_status_t status;
    ucp_request_t *req;

    if (ucs_likely(user_req == NULL)) {
        return UCS_OK;
    } else if (ucs_unlikely(UCS_PTR_IS_ERR(user_req))) {
        ucs_warn("%s failed: %s", op_name,
                 ucs_status_string(UCS_PTR_STATUS(user_req)));
        return UCS_PTR_STATUS(user_req);
    } else {
        req = (ucp_request_t*)user_req - 1;
        do {
            ucp_worker_progress(worker);
        } while (!(req->flags & UCP_REQUEST_FLAG_COMPLETED));
        status = req->status;
        ucp_request_release(user_req);
        return status;
    }
}

static inline void ucp_ep_rma_remote_request_sent(ucp_ep_h ep)
{
    ++ucp_ep_flush_state(ep)->send_sn;
}

static inline void ucp_ep_rma_remote_request_completed(ucp_ep_h ep)
{
    ucp_ep_flush_state_t *flush_state = ucp_ep_flush_state(ep);
    ucp_request_t *req;

    ucp_worker_flush_ops_count_add(ep->worker, -1);
    ++flush_state->cmpl_sn;

    ucs_hlist_for_each_extract_if(req, &flush_state->reqs, send.list,
                                  UCS_CIRCULAR_COMPARE32(
                                          req->send.flush.cmpl_sn, <=,
                                          flush_state->cmpl_sn)) {
        ucp_ep_flush_remote_completed(req);
    }
}

static UCS_F_ALWAYS_INLINE ucs_status_t
ucp_rma_sw_do_am_bcopy(ucp_request_t *req, uint8_t id, ucp_lane_index_t lane,
                       uct_pack_callback_t pack_cb, void *pack_arg,
                       ssize_t *packed_len_p)
{
    ucp_ep_t *ep = req->send.ep;
    ssize_t packed_len;

    /* make an assumption here that EP was able to send the AM, since there
     * are transports (e.g. SELF - it does send-recv in the AM function) that is
     * able to complete the remote request operation inside uct_ep_am_bcopy()
     * and decrement the flush_ops_count before it was incremented */
    ucp_worker_flush_ops_count_add(ep->worker, +1);
    packed_len = uct_ep_am_bcopy(ucp_ep_get_fast_lane(ep, lane),
                                 id, pack_cb, pack_arg, 0);
    if (packed_len > 0) {
        if (packed_len_p != NULL) {
            *packed_len_p = packed_len;
        }
        ucp_ep_rma_remote_request_sent(ep);
        return UCS_OK;
    }

    /* unroll incrementing the flush_ops_count, since uct_ep_am_bcopy()
     * completed with error */
    ucp_worker_flush_ops_count_add(ep->worker, -1);

    return (ucs_status_t)packed_len;
}

static UCS_F_ALWAYS_INLINE ucs_memory_type_t
ucp_amo_request_reply_mem_type(ucp_request_t *req)
{
    return req->send.proto_config->select_param.op.reply.mem_type;
}

static UCS_F_ALWAYS_INLINE size_t
ucp_proto_sw_rma_cfg_thresh(ucp_context_h context, size_t default_value)
{
    return (context->config.ext.prefer_offload == UCS_YES) ?
           UCS_MEMUNITS_INF: /* used only as last resort */
           default_value;
}

static UCS_F_ALWAYS_INLINE int
ucp_ep_rma_is_fence_required(ucp_ep_h ep)
{
    return ep->ext->fence_seq < ep->worker->fence_seq;
}

static UCS_F_ALWAYS_INLINE uint32_t
ucp_ep_rma_get_fence_flag(ucp_ep_h ep)
{
    if (ucs_unlikely(ucp_ep_rma_is_fence_required(ep))) {
        return UCP_REQUEST_FLAG_FENCE_REQUIRED;
    }

    return 0;
}

static UCS_F_ALWAYS_INLINE ucs_status_t
ucp_ep_rma_handle_fence(ucp_ep_h ep, ucp_request_t *req,
                        ucp_lane_map_t lane_map)
{
    ucs_status_t status;

    /* Apply a fence if EP's sequence is behind worker's */
    if (ucs_unlikely(req->flags & UCP_REQUEST_FLAG_FENCE_REQUIRED)) {
        if (ucs_unlikely(ep->ext->unflushed_lanes == 0)) {
            status = UCS_OK;
        } else if (ucs_likely(
            ucs_is_pow2_or_zero(ep->ext->unflushed_lanes | lane_map))) {
            status = ucp_ep_fence_weak(ep);
        } else {
            status = ucp_ep_fence_strong(ep);
        }
    } else {
        status = UCS_OK;
    }

    /* Re-set the lanes of the current operation for future fences */
    ep->ext->unflushed_lanes |= lane_map;

    return status;
}

#endif
