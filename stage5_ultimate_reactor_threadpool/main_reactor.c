#include "main_reactor.h"
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

// 设置非阻塞，因为如果socket是阻塞，
// 一个读写操作就会卡死整个进程
static int set_nonblocking(int fd) {
  // 获取当前fd的状态标志
  int flags = fcntl(fd, F_GETFL, 0);
  if (flags == -1)
    return -1;
  // 在保留原有 flags 的基础上，通过按位或 |
  //  叠加 O_NONBLOCK（非阻塞）标志，再通过 F_SETFL 写回内核。
  return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

// 这是Main-Reactor唯一的事件回调：处理新连接
void main_accept_cb(Event *ev) {
  // 把它还原强转回MainReactor类型，
  //  利用了Event结构体的sub通用指针，便于后续访问
  //  Sub-Reactors数组后轮询索引
  MainReactor *main_r = (MainReactor *)ev->sub;

  // 定义client_addr接收内核填入的client IP和port
  struct sockaddr_in client_addr;
  // client_len记录其结构体大小
  socklen_t client_len = sizeof(client_addr);

  // 尝试从内核的连接队列中提取一个完成三路握手的连接，
  // 返回全新的文件描述符 conn_fd（与客户端通信的 Socket）。
  int conn_fd = accept(ev->fd, (struct sockaddr *)&client_addr, &client_len);
  if (conn_fd < 0) {
    // 因为是非阻塞模式，这里的EAGAIN 和 EWOULDBLOCK表示暂时
    // 没有新连接了，不是真正的错误
    if (errno != EAGAIN && errno != EWOULDBLOCK) {
      perror("accept error");
    }
    return;
  }

  // 打印客户端信息
  char client_ip[INET_ADDRSTRLEN];
  // 将将内核返回的网络字节序（二进制大端）IP 地址转换成我们看得懂的字符串
  inet_ntop(AF_INET, &client_addr.sin_addr, client_ip, sizeof(client_ip));
  // ntohs(client_addr.sin_port)将网络字节序的端口号转为主机字节序（小端/大端自适应）
  printf("[+] [Main-Reactor] 接收到新客户端连接: IP=%s, Port=%d, conn_fd=%d\n",
         client_ip, ntohs(client_addr.sin_port), conn_fd);

  // 取出当前轮到的 Sub-Reactor 下标
  int target_index = main_r->next_sub_index;
  // 获取目标 Sub-Reactor 的指针
  SubReactor *target_sub = &main_r->sub_reactors[target_index];
  // 轮询的核心，每次分配完后下标加 1，并对 SUB_REACTOR_NUM
  // 取模，让索引不断循环。
  main_r->next_sub_index = (main_r->next_sub_index + 1) % SUB_REACTOR_NUM;

  printf("[->] [Main-Reactor] 采用 Round-Robin 算法将 conn_fd=%d 分发给 "
         "Sub-Reactor %d\n",
         conn_fd, target_sub->id);

  // 跨线程调用，把 conn_fd 推进
  // 目标 Sub-Reactor 的线程安全队列，并用 eventfd 唤醒它
  sub_reactor_dispatch_conn(target_sub, conn_fd);
}
// 将 MainReactor 结构体内存空间全部置 0，并将轮询计数器归零
int main_reactor_init(MainReactor *main_r, int port) {
  memset(main_r, 0, sizeof(MainReactor));
  main_r->next_sub_index = 0;

  // 创建包含 4 个线程的共享线程池（全局后厨），
  // 用于处理所有 Sub-Reactor 传递上来的耗时业务。
  main_r->worker_pool = threadpool_create(WORKER_THREAD_NUM);
  if (!main_r->worker_pool) {
    fprintf(stderr, "Failed to create ThreadPool for MainReactor\n");
    return -1;
  }

  // 用循环初始化 4 个 Sub-Reactor，并传入共享线程池的指针
  for (int i = 0; i < SUB_REACTOR_NUM; i++) {
    // sub_reactor_start(...) 内部会调用
    // pthread_create() 启动属于各自的 I/O 线程
    if (sub_reactor_init(&main_r->sub_reactors[i], i, main_r->worker_pool) <
        0) {
      fprintf(stderr, "Failed to init SubReactor %d\n", i);
      return -1;
    }
    sub_reactor_start(&main_r->sub_reactors[i]);
  }

  // 创建 Main-Reactor 的 TCP套接字
  int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (listen_fd < 0) {
    perror("socket error");
    return -1;
  }

  int opt = 1;
  // 开启端口复用。防止服务器重启时，因为内核处于 TIME_WAIT 状态
  //  而报 Address already in use (端口被占用) 的错误
  setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

  struct sockaddr_in server_addr;
  memset(&server_addr, 0, sizeof(server_addr));
  server_addr.sin_family = AF_INET;
  server_addr.sin_addr.s_addr =
      htonl(INADDR_ANY); // INADDR_ANY表示监听所有本机的网卡IP
  server_addr.sin_port = htons(port);

  // bind(...)：将套接字与指定的端口（如 8080）进行绑定
  if (bind(listen_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) <
      0) {
    perror("bind error");
    close(listen_fd);
    return -1;
  }

  // listen(..., SOMAXCONN)：将套接字转为监听模式，
  //  SOMAXCONN 表示内核全连接队列（TCP backlog）允许的最大挂起连接数。
  if (listen(listen_fd, SOMAXCONN) < 0) {
    perror("listen error");
    close(listen_fd);
    return -1;
  }

  // 把监听fd设置为非阻塞。
  set_nonblocking(listen_fd);
  main_r->listen_fd = listen_fd;

  // epoll_create1(0)：向内核申请创建一个
  // 属于 Main-Reactor 的 epoll 树实例，用于监管 listen_fd
  int epoll_fd = epoll_create1(0);
  if (epoll_fd < 0) {
    perror("epoll_create1 failed for MainReactor");
    close(listen_fd);
    return -1;
  }
  main_r->epoll_fd = epoll_fd;

  // 组装Event对象，并挂载到epoll树上
  Event *listen_ev = (Event *)malloc(sizeof(Event));
  memset(listen_ev, 0, sizeof(Event));
  listen_ev->fd = listen_fd; // 绑定当事件触发时要调用的处理函数
  listen_ev->epoll_fd = epoll_fd;
  listen_ev->handler = main_accept_cb; // 绑定前台的回调函数
  listen_ev->sub = (SubReactor *)main_r;

  struct epoll_event ep_ev;
  memset(&ep_ev, 0, sizeof(ep_ev));
  // 监听可读事件（即有新连接）
  ep_ev.events = EPOLLIN;
  // 使用epoll 的联合体特性，将我们的自定义结构体
  //  listen_ev 挂载到 epoll 事件绑定的指针上
  ep_ev.data.ptr = listen_ev;
  // epoll_ctl(..., EPOLL_CTL_ADD, ...)，
  //  将 listen_fd 真正塞进内核的 epoll 红黑树进行监控。
  if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, listen_fd, &ep_ev) < 0) {
    perror("epoll_ctl listen_fd ADD failed for MainReactor");
    close(listen_fd);
    close(epoll_fd);
    free(listen_ev);
    return -1;
  }

  printf("[阶段五] Main-Sub Multi-Reactor + ThreadPool (终极形态) 启动！\n");
  printf("        监听端口: %d | Sub-Reactors (I/O 线程): %d | Worker "
         "ThreadPool (业务线程): %d\n",
         port, SUB_REACTOR_NUM, WORKER_THREAD_NUM);
  return 0;
}

// 主循环：Main-Reactor 线程在这里死循环，不断等待新连接
void main_reactor_run(MainReactor *main_r) {
  struct epoll_event events[MAX_EVENTS];

  while (1) {
    // 阻塞等待，参数 -1 表示如果没有事件发生，线程就在内核中无限期休眠
    // 当有新客户端连接时，内核会唤醒线程，返回就绪的事件数量 nfds
    int nfds = epoll_wait(main_r->epoll_fd, events, MAX_EVENTS, -1);
    if (nfds < 0) {
      if (errno == EINTR) // 软中断信号，不做错误处理，直接continue
        continue;
      perror("main_reactor epoll_wait error");
      break;
    }

    // 遍历所有就绪事件，取出绑定的 Event* 指针，直接调用 ev->handler(ev)。
    // 对于 Main-Reactor，这里调用的实际上就是 main_accept_cb()
    for (int i = 0; i < nfds; i++) {
      Event *ev = (Event *)events[i].data.ptr;
      if (ev && ev->handler) {
        ev->handler(ev); // 触发 main_accept_cb
      }
    }
  }
}

// 逐级资源清理
void main_reactor_destroy(MainReactor *main_r) {
  if (main_r->listen_fd > 0)
    close(main_r->listen_fd);
  if (main_r->epoll_fd > 0)
    close(main_r->epoll_fd);
  for (int i = 0; i < SUB_REACTOR_NUM; i++) {
    sub_reactor_destroy(&main_r->sub_reactors[i]);
  }
  if (main_r->worker_pool) {
    threadpool_destroy(main_r->worker_pool);
  }
}
