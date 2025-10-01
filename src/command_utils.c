#include "command_utils.h"

#include <ctype.h>
#ifdef _WIN32
#include <string.h>
#else
#include <strings.h>
#endif

int command_str_icmp(const char *a, const char *b) {
    if (!a || !b) {
        return (a == b) ? 0 : (a ? 1 : -1);
    }
#ifdef _WIN32
    return _stricmp(a, b);
#else
    return strcasecmp(a, b);
#endif
}
