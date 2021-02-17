/**
 * Copyright (C) Mellanox Technologies Ltd. 2020.  ALL RIGHTS RESERVED.
 *
 * See file LICENSE for terms.
 */

#ifndef UCS_VFS_H_
#define UCS_VFS_H_

#include <ucs/sys/compiler_def.h>
#include <ucs/datastruct/list.h>
#include <ucs/datastruct/string_buffer.h>

BEGIN_C_DECLS

/* This header file defines API for manipulating VFS object tree structure */

typedef struct {
    size_t size;
    int    mode;
} ucs_vfs_path_info_t;


typedef void (*ucs_vfs_file_show_cb_t)(void *obj, ucs_string_buffer_t *strb);

typedef void (*ucs_vfs_refresh_cb_t)(void *obj);

typedef void (*ucs_vfs_list_dir_cb_t)(const char *name, void *arg);


void ucs_vfs_obj_add_dir(void *parent_obj, void *obj, const char *rel_path, ...)
        UCS_F_PRINTF(3, 4);

void ucs_vfs_obj_add_ro_file(void *obj, ucs_vfs_file_show_cb_t text_cb,
                             const char *rel_path, ...) UCS_F_PRINTF(3, 4);

void ucs_vfs_obj_remove(void *obj);

void ucs_vfs_obj_set_dirty(void *obj, ucs_vfs_refresh_cb_t refresh_cb);


/** APIs for the thread */

ucs_status_t ucs_vfs_path_get_info(const char *path, ucs_vfs_path_info_t *info);

ucs_status_t ucs_vfs_path_list_dir(const char *path,
                                   ucs_vfs_list_dir_cb_t dir_cb, void *arg);

char *ucs_vfs_path_read_file(const char *path);

END_C_DECLS

#endif
