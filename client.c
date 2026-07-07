#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#define BUFFER_SIZE 1024
#define SERVER_FIFO "server_fifo"

static char name[50];
static char client_fifo[256];

void *receive_messages(void *arg) {
    (void)arg;
    int fd = open(client_fifo, O_RDONLY);
    if (fd == -1) {
        perror("open client fifo for read");
        return NULL;
    }
    char msg[BUFFER_SIZE];
    ssize_t len;
    while ((len = read(fd, msg, sizeof(msg) - 1)) > 0) {
        msg[len] = '\0';
        printf("%s", msg);
        fflush(stdout);
    }
    close(fd);
    return NULL;
}

int main() {
    pthread_t recv_thread;

    printf("Enter your name: ");
    if (!fgets(name, sizeof(name), stdin)) return 1;
    name[strcspn(name, "\n")] = 0;

    // Create a unique client fifo path using pid
    snprintf(client_fifo, sizeof(client_fifo), "client_fifo_%d", getpid());
    unlink(client_fifo);
    if (mkfifo(client_fifo, 0666) == -1) {
        perror("mkfifo client");
        return 1;
    }

    // Join: write JOIN <fifo> <name> to server fifo
    int server_fd = open(SERVER_FIFO, O_WRONLY);
    if (server_fd == -1) {
        perror("open server fifo");
        unlink(client_fifo);
        return 1;
    }
    char join_cmd[BUFFER_SIZE + 256];
    snprintf(join_cmd, sizeof(join_cmd), "JOIN %s %s\n", client_fifo, name);
    write(server_fd, join_cmd, strlen(join_cmd));

    // Start receiver thread
    pthread_create(&recv_thread, NULL, receive_messages, NULL);

    // Read stdin and send as MSG
    char message[BUFFER_SIZE];
    while (1) {
        if (!fgets(message, sizeof(message), stdin)) break;
        if (strcmp(message, "exit\n") == 0) {
            break;
        }
        char msg_cmd[BUFFER_SIZE + 64];
        snprintf(msg_cmd, sizeof(msg_cmd), "MSG %s: %s", name, message);
        write(server_fd, msg_cmd, strlen(msg_cmd));
    }

    // Leave
    char leave_cmd[BUFFER_SIZE];
    snprintf(leave_cmd, sizeof(leave_cmd), "LEAVE %s\n", client_fifo);
    write(server_fd, leave_cmd, strlen(leave_cmd));
    close(server_fd);

    // Give receiver a moment then cleanup
    usleep(50000);
    unlink(client_fifo);
    return 0;
}

