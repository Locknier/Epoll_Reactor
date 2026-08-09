#include "http_parser.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

// 去除字符串前后空白字符
static char* trim(char *str) {
    while (isspace((unsigned char)*str)) str++;
    if (*str == 0) return str;
    char *end = str + strlen(str) - 1;
    while (end > str && isspace((unsigned char)*end)) end--;
    end[1] = '\0';
    return str;
}

int parse_http_request(const char *raw_buf, int len, HttpRequest *req) {
    if (!raw_buf || len <= 0 || !req) return -1;
    memset(req, 0, sizeof(HttpRequest));

    // 复制缓冲区以进行 safe strtok / line parsing
    char *buf_copy = (char *)malloc(len + 1);
    if (!buf_copy) return -1;
    memcpy(buf_copy, raw_buf, len);
    buf_copy[len] = '\0';

    // 1. 解析请求行 (Request Line: METHOD PATH VERSION)
    char *line_save = NULL;
    char *line = strtok_r(buf_copy, "\r\n", &line_save);
    if (!line) {
        free(buf_copy);
        return -1;
    }

    char method[MAX_METHOD_LEN] = {0};
    char path[MAX_PATH_LEN] = {0};
    char version[MAX_VERSION_LEN] = {0};

    if (sscanf(line, "%15s %255s %15s", method, path, version) != 3) {
        free(buf_copy);
        return -1;
    }

    strncpy(req->method, method, MAX_METHOD_LEN - 1);
    strncpy(req->path, path, MAX_PATH_LEN - 1);
    strncpy(req->version, version, MAX_VERSION_LEN - 1);

    // 2. 解析 Headers
    while ((line = strtok_r(NULL, "\r\n", &line_save)) != NULL) {
        if (strlen(line) == 0) {
            // 空行代表 Headers 结束，后续为 Body
            break;
        }

        char *colon = strchr(line, ':');
        if (colon && req->header_count < MAX_HEADERS) {
            *colon = '\0';
            char *key = trim(line);
            char *val = trim(colon + 1);

            strncpy(req->headers[req->header_count].key, key, MAX_HEADER_KEY_LEN - 1);
            strncpy(req->headers[req->header_count].value, val, MAX_HEADER_VAL_LEN - 1);
            req->header_count++;
        }
    }

    // 3. 查找 Body (如果有 Content-Length)
    const char *header_end = strstr(raw_buf, "\r\n\r\n");
    if (header_end) {
        req->body = header_end + 4;
        req->body_length = len - (req->body - raw_buf);
        if (req->body_length < 0) req->body_length = 0;
    }

    free(buf_copy);
    return 0;
}

const char* get_http_header(const HttpRequest *req, const char *key) {
    if (!req || !key) return NULL;
    for (int i = 0; i < req->header_count; i++) {
        if (strcasecmp(req->headers[i].key, key) == 0) {
            return req->headers[i].value;
        }
    }
    return NULL;
}

int build_http_response(const HttpResponse *res, char *out_buf, int out_max_len) {
    if (!res || !out_buf || out_max_len <= 0) return -1;

    const char *status_phrase = res->status_phrase ? res->status_phrase : "OK";
    const char *content_type = res->content_type ? res->content_type : "text/html; charset=utf-8";
    const char *body = res->body ? res->body : "";
    int body_len = res->body_length > 0 ? res->body_length : (int)strlen(body);

    int head_len = snprintf(out_buf, out_max_len,
        "HTTP/1.1 %d %s\r\n"
        "Server: Epoll-MultiReactor-HttpServer/1.0\r\n"
        "Content-Type: %s\r\n"
        "Content-Length: %d\r\n"
        "Connection: close\r\n"
        "\r\n",
        res->status_code,
        status_phrase,
        content_type,
        body_len
    );

    if (head_len < 0 || head_len >= out_max_len) return -1;

    if (body_len > 0 && head_len + body_len < out_max_len) {
        memcpy(out_buf + head_len, body, body_len);
    }

    return head_len + body_len;
}
