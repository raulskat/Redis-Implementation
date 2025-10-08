#ifndef SDS_H
#define SDS_H

#include <stddef.h>

typedef char *sds;

sds sdsnewlen(const void *init, size_t initlen);
sds sdsnew(const char *init);
sds sdsempty(void);
sds sdsdup(const sds s);
void sdsfree(sds s);
size_t sdslen(const sds s);

#endif /* SDS_H */
