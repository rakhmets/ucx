/**
 * Copyright (C) Mellanox Technologies Ltd. 2020.  ALL RIGHTS RESERVED.
 *
 * See file LICENSE for terms.
 */

#ifdef HAVE_CONFIG_H
#  include "config.h"
#endif

#include "vfs_obj.h"

#include <ucs/datastruct/khash.h>
#include <ucs/debug/assert.h>
#include <ucs/debug/memtrack.h>
#include <ucs/debug/log.h>
#include <ucs/sys/compiler.h>
#include <ucs/sys/string.h>
#include <ucs/type/spinlock.h>
#include <sys/stat.h>
#include <string.h>
#include <stdarg.h>
#include <stdio.h>


typedef enum {
    UCS_VFS_NODE_TYPE_DIR,
    UCS_VFS_NODE_TYPE_RO_FILE,
    UCS_VFS_NODE_TYPE_SUBDIR,
    UCS_VFS_NODE_TYPE_LAST
} ucs_vfs_node_type_t;


typedef struct ucs_vfs_node ucs_vfs_node_t;
struct ucs_vfs_node {
    ucs_vfs_node_type_t    type;
    int16_t                refcount;
    uint8_t                dirty;

    void                   *obj;
    ucs_vfs_node_t         *parent;
    ucs_list_link_t        children;
    ucs_vfs_file_show_cb_t text_cb;
    ucs_vfs_refresh_cb_t   refresh_cb;
    ucs_list_link_t        list;
    char                   path[0];
};

KHASH_MAP_INIT_STR(vfs_path, ucs_vfs_node_t*);
KHASH_MAP_INIT_INT64(vfs_obj, ucs_vfs_node_t*);

struct {
    ucs_spinlock_t    lock;
    ucs_vfs_node_t    root;
    khash_t(vfs_path) path_hash;
    khash_t(vfs_obj)  obj_hash;
} ucs_vfs_obj_context = {};


/* must be called with lock held */
static ucs_vfs_node_t *ucs_vfs_node_find_by_obj(void *obj)
{
    khiter_t khiter = kh_get(vfs_obj, &ucs_vfs_obj_context.obj_hash,
                             (uintptr_t)obj);
    ucs_vfs_node_t *node;

    if (khiter == kh_end(&ucs_vfs_obj_context.obj_hash)) {
        return NULL;
    }

    node = kh_val(&ucs_vfs_obj_context.obj_hash, khiter);
    ucs_assert(node->obj == obj);
    return node;
}

/* must be called with lock held */
static ucs_vfs_node_t *ucs_vfs_node_find_by_path(const char *path)
{
    khiter_t khiter = kh_get(vfs_path, &ucs_vfs_obj_context.path_hash, path);
    ucs_vfs_node_t *node;

    if (khiter == kh_end(&ucs_vfs_obj_context.path_hash)) {
        return NULL;
    }

    node = kh_val(&ucs_vfs_obj_context.path_hash, khiter);
    ucs_assert(!strcmp(node->path, path));
    return node;
}

/* must be called with lock held */
static void ucs_vfs_node_init(ucs_vfs_node_t *node)
{
    node->type       = UCS_VFS_NODE_TYPE_LAST;
    node->refcount   = 1;
    node->dirty      = 0;
    node->obj        = NULL;
    node->parent     = NULL;
    node->text_cb    = NULL;
    node->refresh_cb = NULL;
    ucs_list_head_init(&node->children);
}

/* must be called with lock held */
static ucs_vfs_node_t *
ucs_vfs_node_create(ucs_vfs_node_t *parent_node, const char *name,
                    ucs_vfs_node_type_t type, void *obj)
{
    char path_buf[PATH_MAX];
    ucs_vfs_node_t *node;
    khiter_t khiter;
    int khret;

    if (parent_node == &ucs_vfs_obj_context.root) {
        ucs_snprintf_safe(path_buf, sizeof(path_buf), "/%s", name);
    } else {
        ucs_snprintf_safe(path_buf, sizeof(path_buf), "%s/%s",
                          parent_node->path, name);
    }

    node = ucs_vfs_node_find_by_path(path_buf);
    if (node != NULL) {
        return node;
    }

    node = ucs_malloc(sizeof(*node) + strlen(path_buf) + 1, "vfs_node");
    if (node == NULL) {
        return NULL;
    }

    /* initialize node */
    ucs_vfs_node_init(node);
    node->type   = type;
    node->obj    = obj;
    node->parent = parent_node;
    strcpy(node->path, path_buf);

    /* add to parent */
    ucs_list_add_head(&parent_node->children, &node->list);

    /* add to obj hash */
    if (node->obj != NULL) {
        khiter = kh_put(vfs_obj, &ucs_vfs_obj_context.obj_hash,
                        (uintptr_t)node->obj, &khret);
        ucs_assert((khret == UCS_KH_PUT_BUCKET_EMPTY) ||
                   (khret == UCS_KH_PUT_BUCKET_CLEAR));
        kh_val(&ucs_vfs_obj_context.obj_hash, khiter) = node;
    }

    /* add to path hash */
    khiter = kh_put(vfs_path, &ucs_vfs_obj_context.path_hash, node->path,
                    &khret);
    ucs_assert((khret == UCS_KH_PUT_BUCKET_EMPTY) ||
               (khret == UCS_KH_PUT_BUCKET_CLEAR));
    kh_val(&ucs_vfs_obj_context.path_hash, khiter) = node;

    return node;
}

/* must be called with lock held */
static void ucs_vfs_node_get(ucs_vfs_node_t *node)
{
    ++node->refcount;
}

/* must be called with lock held */
static void ucs_vfs_node_put(ucs_vfs_node_t *node)
{
    ucs_vfs_node_t *parent_node = node->parent;
    ucs_vfs_node_t *child_node;
    khiter_t khiter;

    if (--node->refcount > 0) {
        return;
    }

    /* recursively remove children empty parent subdirs */
    ucs_list_for_each(child_node, &node->children, list) {
        child_node->parent = NULL; /* prevent children from destroying me */
        ucs_vfs_node_put(child_node);
    }

    /* remove from object hash */
    if (node->obj != NULL) {
        khiter = kh_get(vfs_obj, &ucs_vfs_obj_context.obj_hash,
                        (uintptr_t)node->obj);
        ucs_assert(khiter != kh_end(&ucs_vfs_obj_context.obj_hash));
        kh_del(vfs_obj, &ucs_vfs_obj_context.obj_hash, khiter);
    }

    /* remove from path hash */
    khiter = kh_get(vfs_path, &ucs_vfs_obj_context.path_hash, node->path);
    ucs_assert(khiter != kh_end(&ucs_vfs_obj_context.path_hash));
    kh_del(vfs_path, &ucs_vfs_obj_context.path_hash, khiter);

/* TODO implement VFS object tree structure */
    /* remove from parent's list */
    ucs_list_del(&node->list);

    ucs_free(node);

    /* recursively remove all empty parent subdirs */
    if ((parent_node != NULL) && ucs_list_is_empty(&parent_node->children) &&
        (parent_node->type == UCS_VFS_NODE_TYPE_SUBDIR)) {
        ucs_vfs_node_put(node->parent);
    }
}

/* must be called with lock held */
static ucs_vfs_node_t *
ucs_vfs_node_add(void *parent_obj, ucs_vfs_node_type_t type, void *obj,
                 const char *rel_path, va_list ap)
{
    ucs_vfs_node_t *parent_node;
    char rel_path_buf[PATH_MAX];
    char *token, *next_token;

    if (parent_obj == NULL) {
        parent_node = &ucs_vfs_obj_context.root;
    } else {
        parent_node = ucs_vfs_node_find_by_obj(parent_obj);
        if (parent_node == NULL) {
            return NULL;
        }
    }

    /* generate the relative path */
    // TODO ucs_vsnprintf_safe
    vsnprintf(rel_path_buf, sizeof(rel_path_buf), rel_path, ap);
    rel_path_buf[sizeof(rel_path_buf) - 1] = '\0';

    /* Build parent nodes along the rel_path, without associated object */
    next_token = rel_path_buf;
    token      = strsep(&next_token, "/");
    while (next_token != NULL) {
        parent_node = ucs_vfs_node_create(parent_node, token,
                                          UCS_VFS_NODE_TYPE_SUBDIR, NULL);
        token       = strsep(&next_token, "/");
    }

    return ucs_vfs_node_create(parent_node, token, type, obj);
}

void ucs_vfs_obj_add_dir(void *parent_obj, void *obj, const char *rel_path, ...)
{
    va_list ap;

    ucs_spin_lock(&ucs_vfs_obj_context.lock);
    va_start(ap, rel_path);
    ucs_vfs_node_add(parent_obj, UCS_VFS_NODE_TYPE_DIR, obj, rel_path, ap);
    va_end(ap);
    ucs_spin_unlock(&ucs_vfs_obj_context.lock);
}

void ucs_vfs_obj_add_ro_file(void *obj, ucs_vfs_file_show_cb_t text_cb,
                             const char *rel_path, ...)
{
    ucs_vfs_node_t *node;
    va_list ap;

    ucs_spin_lock(&ucs_vfs_obj_context.lock);

    va_start(ap, rel_path);
    node = ucs_vfs_node_add(obj, UCS_VFS_NODE_TYPE_RO_FILE, NULL, rel_path, ap);
    va_end(ap);

    if (node != NULL) {
        node->text_cb = text_cb;
    }

    ucs_spin_unlock(&ucs_vfs_obj_context.lock);
}

void ucs_vfs_obj_remove(void *obj)
{
    ucs_vfs_node_t *node;

    ucs_spin_lock(&ucs_vfs_obj_context.lock);
    node = ucs_vfs_node_find_by_obj(obj);
    if (node != NULL) {
        ucs_vfs_node_put(node);
    }
    ucs_spin_unlock(&ucs_vfs_obj_context.lock);
}

void ucs_vfs_obj_set_dirty(void *obj, ucs_vfs_refresh_cb_t refresh_cb)
{
    ucs_vfs_node_t *node = ucs_vfs_node_find_by_obj(obj);

    ucs_spin_lock(&ucs_vfs_obj_context.lock);
    if (node != NULL) {
        node->dirty      = 1;
        node->refresh_cb = refresh_cb;
    }
    ucs_spin_unlock(&ucs_vfs_obj_context.lock);
}

/* must be called with lock held and incremented refcount */
static void ucs_vfs_dir_refresh(ucs_vfs_node_t *node)
{
    if (!node->dirty) {
        return;
    }

    ucs_assert(node->refcount >= 2);

    ucs_spin_unlock(&ucs_vfs_obj_context.lock);
    node->refresh_cb(node->obj);
    ucs_spin_lock(&ucs_vfs_obj_context.lock);

    node->dirty = 0;
}

ucs_status_t ucs_vfs_path_get_info(const char *path, ucs_vfs_path_info_t *info)
{
    ucs_string_buffer_t strb;
    ucs_vfs_node_t *node;
    ucs_status_t status;

    ucs_spin_lock(&ucs_vfs_obj_context.lock);
    node = ucs_vfs_node_find_by_path(path);
    if (node == NULL) {
        status = UCS_ERR_NO_ELEM;
        goto out_unlock;
    }

    ucs_vfs_node_get(node);

    switch (node->type) {
    case UCS_VFS_NODE_TYPE_DIR:
    case UCS_VFS_NODE_TYPE_SUBDIR:
        ucs_vfs_dir_refresh(node);
        info->mode = S_IFDIR | S_IRUSR | S_IXUSR;
        info->size = ucs_list_length(&node->children);
        status     = UCS_OK;
        break;
    case UCS_VFS_NODE_TYPE_RO_FILE:
        ucs_string_buffer_init(&strb);
        node->text_cb(node->parent->obj, &strb);
        info->mode = S_IFREG | S_IRUSR;
        info->size = ucs_string_buffer_length(&strb);
        ucs_string_buffer_cleanup(&strb);
        status = UCS_OK;
        break;
    default:
        status = UCS_ERR_NO_ELEM;
        break;
    }

    ucs_vfs_node_put(node);

out_unlock:
    ucs_spin_unlock(&ucs_vfs_obj_context.lock);
    return status;
}

ucs_status_t
ucs_vfs_path_list_dir(const char *path, ucs_vfs_list_dir_cb_t dir_cb, void *arg)
{
    ucs_vfs_node_t *node, *child_node;
    ucs_status_t status;

    ucs_spin_lock(&ucs_vfs_obj_context.lock);

    if (!strcmp(path, "/")) {
        node = &ucs_vfs_obj_context.root;
    } else {
        node = ucs_vfs_node_find_by_path(path);
    }

    if ((node == NULL) || ((node->type != UCS_VFS_NODE_TYPE_DIR) &&
                           (node->type != UCS_VFS_NODE_TYPE_SUBDIR))) {
        status = UCS_ERR_NO_ELEM;
        goto out_unlock;
    }

    ucs_vfs_dir_refresh(node);
    ucs_list_for_each(child_node, &node->children, list) {
        dir_cb(ucs_basename(child_node->path), arg);
    }
    status = UCS_OK;

out_unlock:
    ucs_spin_unlock(&ucs_vfs_obj_context.lock);
    return status;
}

char *ucs_vfs_path_read_file(const char *path)
{
    ucs_string_buffer_t strb;
    ucs_vfs_node_t *node;
    char *result;

    ucs_spin_lock(&ucs_vfs_obj_context.lock);
    node = ucs_vfs_node_find_by_path(path);
    if ((node == NULL) || (node->type != UCS_VFS_NODE_TYPE_RO_FILE)) {
        ucs_spin_unlock(&ucs_vfs_obj_context.lock);
        return NULL;
    }

    ucs_vfs_node_get(node);
    ucs_spin_unlock(&ucs_vfs_obj_context.lock);

    ucs_string_buffer_init(&strb);
    node->text_cb(node->parent->obj, &strb);
    result = ucs_strdup(ucs_string_buffer_cstr(&strb), "vfs_text");
    ucs_string_buffer_cleanup(&strb);

    ucs_spin_lock(&ucs_vfs_obj_context.lock);
    ucs_vfs_node_put(node);
    ucs_spin_unlock(&ucs_vfs_obj_context.lock);

    return result;
}

UCS_STATIC_INIT
{
    ucs_spinlock_init(&ucs_vfs_obj_context.lock, 0);
    ucs_vfs_node_init(&ucs_vfs_obj_context.root);
    ucs_vfs_obj_context.root.type = UCS_VFS_NODE_TYPE_DIR;
    kh_init_inplace(vfs_obj, &ucs_vfs_obj_context.obj_hash);
    kh_init_inplace(vfs_path, &ucs_vfs_obj_context.path_hash);
}

UCS_STATIC_CLEANUP
{
    // TODO
}