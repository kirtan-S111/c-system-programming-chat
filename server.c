#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>
#include <time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#define MAX_CLIENTS 20
#define BUFFER_SIZE 1024
#define LOG_FILE "chat_log.txt"
#define SERVER_FIFO "server_fifo"

typedef struct {
    char fifo_path[256];
    char name[64];
    int active;
} ClientInfo;

pthread_mutex_t clients_mutex = PTHREAD_MUTEX_INITIALIZER;
ClientInfo clients[MAX_CLIENTS];
int num_clients = 0;

// Write chat logs with timestamp
void log_message(const char *msg) {
    FILE *fp = fopen(LOG_FILE, "a");
    if (!fp) return;
    time_t now = time(0);
    char *time_str = ctime(&now);
    time_str[strcspn(time_str, "\n")] = 0;
    fprintf(fp, "[%s] %s\n", time_str, msg);
    fclose(fp);
}

// Find client index by fifo path
int find_client_index_by_fifo(const char *fifo_path) {
    for (int i = 0; i < num_clients; i++) {
        if (clients[i].active && strcmp(clients[i].fifo_path, fifo_path) == 0) {
            return i;
        }
    }
    return -1;
}

// Broadcast message to all clients
void broadcast_message(const char *msg) {
    pthread_mutex_lock(&clients_mutex);
    for (int i = 0; i < num_clients; i++) {
        if (!clients[i].active) continue;
        int fd = open(clients[i].fifo_path, O_WRONLY | O_NONBLOCK);
        if (fd == -1) {
            perror("open client fifo");
            continue;
        }
        write(fd, msg, strlen(msg));
        close(fd);
    }
    pthread_mutex_unlock(&clients_mutex);
    log_message(msg);
}

// Handle a single command line from server fifo
void handle_command(char *line) {
    // Expected commands:
    // JOIN <fifo_path> <name>
    // LEAVE <fifo_path>
    // MSG <name> <text...>
    if (strncmp(line, "JOIN ", 5) == 0) {
        char fifo_path[256];
        char name[64];
        if (sscanf(line + 5, "%255s %63[^\n]", fifo_path, name) == 2) {
            pthread_mutex_lock(&clients_mutex);
            if (num_clients < MAX_CLIENTS) {
                strncpy(clients[num_clients].fifo_path, fifo_path, sizeof(clients[num_clients].fifo_path) - 1);
                clients[num_clients].fifo_path[sizeof(clients[num_clients].fifo_path) - 1] = '\0';
                strncpy(clients[num_clients].name, name, sizeof(clients[num_clients].name) - 1);
                clients[num_clients].name[sizeof(clients[num_clients].name) - 1] = '\0';
                clients[num_clients].active = 1;
                num_clients++;
            }
            pthread_mutex_unlock(&clients_mutex);

            char join_msg[BUFFER_SIZE];
            snprintf(join_msg, sizeof(join_msg), "%s joined the chat.\n", name);
            printf("%s", join_msg);
            broadcast_message(join_msg);
        }
    } else if (strncmp(line, "LEAVE ", 7) == 0) {
        char fifo_path[256];
        if (sscanf(line + 7, "%255s", fifo_path) == 1) {
            pthread_mutex_lock(&clients_mutex);
            int idx = find_client_index_by_fifo(fifo_path);
            char name[64] = "someone";
            if (idx >= 0) {
                strncpy(name, clients[idx].name, sizeof(name) - 1);
                name[sizeof(name) - 1] = '\0';
                clients[idx] = clients[num_clients - 1];
                num_clients--;
            }
            pthread_mutex_unlock(&clients_mutex);

            char leave_msg[BUFFER_SIZE];
            snprintf(leave_msg, sizeof(leave_msg), "%s left the chat.\n", name);
            printf("%s", leave_msg);
            broadcast_message(leave_msg);
        }
    } else if (strncmp(line, "MSG ", 4) == 0) {
        // Forward as-is after removing leading "MSG "
        char *payload = line + 4;
        printf("%s", payload);
        broadcast_message(payload);
    }
}

int main() {
    // Create server fifo if not exists
    unlink(SERVER_FIFO);
    if (mkfifo(SERVER_FIFO, 0666) == -1) {
        // If creation fails due to existing, continue
        // But unlink above should clear old one
    }

    // Open server fifo for reading; keep a dummy writer open so reads don't get EOF
    int server_fd = open(SERVER_FIFO, O_RDONLY | O_NONBLOCK);
    if (server_fd == -1) {
        perror("open server fifo for read");
        exit(EXIT_FAILURE);
    }
    int server_fd_dummy = open(SERVER_FIFO, O_WRONLY | O_NONBLOCK);

    printf("Server started with FIFO '%s'\n", SERVER_FIFO);
    log_message("Server started (FIFO mode)...");

    char buffer[BUFFER_SIZE];
    while (1) {
        ssize_t n = read(server_fd, buffer, sizeof(buffer) - 1);
        if (n > 0) {
            buffer[n] = '\0';
            // The read may contain multiple lines; process each line
            char *saveptr = NULL;
            char *line = strtok_r(buffer, "\n", &saveptr);
            while (line) {
                handle_command(line);
                line = strtok_r(NULL, "\n", &saveptr);
            }
        } else {
            // No data; sleep briefly to avoid busy loop
            usleep(10000);
        }
    }

    if (server_fd_dummy != -1) close(server_fd_dummy);
    close(server_fd);
    unlink(SERVER_FIFO);
    return 0;
}
