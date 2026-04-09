/**
 * Copyright (c) NVIDIA CORPORATION & AFFILIATES, 2026. ALL RIGHTS RESERVED.
 *
 * See file LICENSE for terms.
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "test_cuda_check_def.h"
#include "test_mpi_tags_def.h"
#include "test_ucp.h"
#include "test_ucx_check_def.h"

#include <ucp/api/device/ucp_host.h>

#include <cuda.h>
#include <mpi.h>

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#define MPI_COMM_SIZE 2
#define SIZE          1024 * 1024 * 1024

static void rank0(ucp_context_h ucp_context)
{
    CUdeviceptr ptr;
    ucp_mem_h ucp_mem;
    size_t free_bytes_before, total_bytes, free_bytes_after, unreleased_memory;

    CUDA_CHECK(cuMemGetInfo(&free_bytes_before, &total_bytes));
    CUDA_CHECK(cuMemAlloc(&ptr, SIZE));

    ucp_mem = send_rkey(1, (void*)ptr, SIZE, ucp_context);

    MPI_Barrier(MPI_COMM_WORLD);

    UCX_CHECK(ucp_mem_unmap(ucp_context, ucp_mem));
    CUDA_CHECK(cuMemFree(ptr));
    CUDA_CHECK(cuCtxSynchronize());

    CUDA_CHECK(cuMemGetInfo(&free_bytes_after, &total_bytes));
    unreleased_memory = free_bytes_before - free_bytes_after;
    fprintf(stdout, "Unreleased memory: %zu bytes: %s\n", unreleased_memory,
            (unreleased_memory == 0) ? "PASS" : "FAIL");
}

static void rank1(ucp_ep_h ucp_ep)
{
    rkey_t rkey;
    void *ptr;
    ucp_device_mem_list_elem_t device_mem_list_elem;
    ucp_device_mem_list_params_t device_mem_list_params;
    ucp_device_remote_mem_list_h device_remote_mem_list;

    rkey = recv_rkey(0, ucp_ep);

    UCX_CHECK(ucp_rkey_ptr(rkey.rkey, rkey.remote_address, &ptr));

    device_mem_list_elem.field_mask =
            UCP_DEVICE_MEM_LIST_ELEM_FIELD_REMOTE_ADDR |
            UCP_DEVICE_MEM_LIST_ELEM_FIELD_RKEY |
            UCP_DEVICE_MEM_LIST_ELEM_FIELD_EP;
    device_mem_list_elem.remote_addr = rkey.remote_address;
    device_mem_list_elem.rkey        = rkey.rkey;
    device_mem_list_elem.ep          = ucp_ep;

    device_mem_list_params.field_mask =
            UCP_DEVICE_MEM_LIST_PARAMS_FIELD_ELEMENT_SIZE |
            UCP_DEVICE_MEM_LIST_PARAMS_FIELD_NUM_ELEMENTS |
            UCP_DEVICE_MEM_LIST_PARAMS_FIELD_ELEMENTS;
    device_mem_list_params.element_size = sizeof(device_mem_list_elem);
    device_mem_list_params.num_elements = 1;
    device_mem_list_params.elements     = &device_mem_list_elem;

    UCX_CHECK(ucp_device_remote_mem_list_create(&device_mem_list_params,
                                                &device_remote_mem_list));
    ucp_device_mem_list_release(device_remote_mem_list);

    ucp_rkey_destroy(rkey.rkey);

    MPI_Barrier(MPI_COMM_WORLD);
}

int main(int argc, char **argv)
{
    int comm_size, rank;
    int cu_dev_count;
    CUdevice cu_dev;
    CUcontext cu_ctx;
    ucp_t ucp;

    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &comm_size);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    if (comm_size != MPI_COMM_SIZE) {
        if (rank == 0) {
            fprintf(stderr, "This test requires exactly %d MPI processes\n",
                    MPI_COMM_SIZE);
        }
        MPI_Finalize();
        return EXIT_FAILURE;
    }

    CUDA_CHECK(cuInit(0));
    CUDA_CHECK(cuDeviceGetCount(&cu_dev_count));
    CUDA_CHECK(cuDeviceGet(&cu_dev, rank % cu_dev_count));
    CUDA_CHECK(cuDevicePrimaryCtxRetain(&cu_ctx, cu_dev));
    CUDA_CHECK(cuCtxSetCurrent(cu_ctx));

    ucp = create_ucp(UCP_FEATURE_RMA | UCP_FEATURE_DEVICE);

    if (rank == 0) {
        rank0(ucp.context);
    } else {
        rank1(ucp.ep);
    }

    MPI_Barrier(MPI_COMM_WORLD);

    destroy_ucp(ucp);

    CUDA_CHECK(cuCtxPopCurrent(NULL));
    CUDA_CHECK(cuDevicePrimaryCtxRelease(cu_dev));

    MPI_Finalize();
    return EXIT_SUCCESS;
}
