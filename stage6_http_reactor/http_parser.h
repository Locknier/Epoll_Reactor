#ifndef HTTP_PARSER_H
#define HTTP_PARSER_H

#define MAX_METHOD_LEN 16
#define MAX_PATH_LEN 256
#define MAX_VERSION_LEN 16
#define MAX_HEADERS 32
#define MAX_HEADER_KEY_LEN 64
#define MAX_HEADER_VAL_LEN 256

typedef struct {
    char key[MAX_HEADER_KEY_LEN];
    char value[MAX_HEADER_VAL_LEN];
} HttpHeader;

typedef struct {
    char method[MAX_METHOD_LEN];
    char path[MAX_PATH_LEN];
    char version[MAX_VERSION_LEN];
    HttpHeader headers[MAX_HEADERS];
    int header_count;
    const char *body;
    int body_length;
} HttpRequest;

typedef struct {
    int status_code;
    const char *status_phrase;
    const char *content_type;
    const char *body;
    int body_length;
} HttpResponse;

// 解析 HTTP 请求
int parse_http_request(const char *raw_buf, int len, HttpRequest *req);

// 构建 HTTP 响应报文
int build_http_response(const HttpResponse *res, char *out_buf, int out_max_len);

// 查找指定 Header 的值
const char* get_http_header(const HttpRequest *req, const char *key);

#endif // HTTP_PARSER_H
