#include <ctype.h>
#include <errno.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/select.h>
#include <sys/socket.h>
#include<stdbool.h>
#include <unistd.h>
#include <sys/time.h>
#include <time.h>
#include <pthread.h>
#include <stdint.h>
#include <arpa/inet.h> 
#define MAX_CONNECTIONS 10
#define MAX_ENTRY_STR_SIZE 256
#define MAX_MAP_SIZE 1000
#define SELECTDB 0xFE
#define RESIZEDB 0xFB
#define AUX 0xFA
#define EXPIRETIME 0xFD
#define EXPIRETIMEMS 0xFC
#define EOF_OP 0xFF
#define KEYVALUE 0x00

typedef struct {
    char key[MAX_ENTRY_STR_SIZE];
    char value[MAX_ENTRY_STR_SIZE];
    uint64_t time_set;
    uint64_t expiry;
} redis_map_entry;

typedef struct {
    redis_map_entry *map[MAX_MAP_SIZE];
    size_t size;
} redis_map_t;

bool slave = false;
char master_host_name[100];
int master_port=6379;
int port=6379;

uint64_t get_current_time();

char rdb_dir[MAX_ENTRY_STR_SIZE] = "/tmp/rdbfile";     // Default directory
char rdb_filename[MAX_ENTRY_STR_SIZE] = "dump.rdb"; 
char replication_role[MAX_ENTRY_STR_SIZE]="master";



redis_map_t redis_map;
uint64_t readLengthEncoding(FILE *file) {
  unsigned char buf;
  fread(&buf, sizeof(buf), 1, file);
  switch (buf >> 6) {
  case 0:
    return buf & 0x3F;
  case 1: {
    uint64_t len = buf & 0x3F;
    fread(&buf, sizeof(buf), 1, file);
    return (len << 8) | buf;
  }
  case 2: {
    uint64_t len;
    fread(&len, sizeof(len), 1, file);
    return len;
  }
  default:
    return 0; // Special format not handled
  }
}
// Function to read a string encoded value
char *readString(FILE *file) {
  uint64_t length = readLengthEncoding(file);
  char *str = malloc(length + 1);
  fread(str, sizeof(char), length, file);
  str[length] = '\0';
  return str;
}
void set_entry(const char *key, const char *value,uint64_t expiry) {
    // Allocate memory for the new entry
    redis_map_entry *entry = (redis_map_entry *)malloc(sizeof(redis_map_entry));
                    strcpy(entry->key, key);
                    strcpy(entry->value, value);
                    entry->time_set = 0;
                    entry->expiry = expiry;
                    redis_map.map[redis_map.size++] = entry;
                    printf("Setting key: %s, value: %s, expiry: %lld \n,", key, value, expiry);
  printf("Key set\n");
}

// Function to parse a key-value pair
void parse_key_value(FILE *rdb_file) {
  // Read value type
  unsigned char value_type;
  fread(&value_type, sizeof(unsigned char), 1, rdb_file);
  // Parse the value based on the value type
   printf("The value in hexadecimal is: 0x%02x\n", value_type);
    uint64_t expiry = 0;
  switch (value_type) {
 case KEYVALUE: { // String encoding
    expiry=0;
    char *key = readString(rdb_file);// Parse the key as a string
    char *value = readString(rdb_file);
    printf("Key: %s, Value: %s \n", key, value);
    set_entry(key,value,expiry);
                    
    break;
  }
  
  case EXPIRETIMEMS: {
    fread(&expiry, sizeof(char), 8, rdb_file);
    fread(&value_type, sizeof(unsigned char), 1, rdb_file);
    printf("Value type: %02X\n", value_type);
    if (value_type == KEYVALUE) {
      char *key = readString(rdb_file);
      char *value = readString(rdb_file);
      printf("Key: %s, Value: %s, Expiry: %lu milliseconds\n", key, value,
             expiry);
      set_entry(key, value, expiry);
    }
    break;
  }
  default:
    printf("Unsupported value type\n");
    break;
  }
}

// Function to parse RDB file
void parseRDBFile(const char *filename) {
  char path[256];
  snprintf(path, sizeof(path), "%s", filename);
  FILE *file = fopen(path, "rb");
  if (!file) {
    printf("RDB file not found at %s. Treating the database as empty.\n", path);
   // Ensure map size is set to 0, treating the database as empty
        return;
  }
  // Read the initial head                      
  char magic[6];
  fread(magic, sizeof(char), 5, file);
  magic[5] = '\0';
  printf("Magic: %s\n", magic);
  if (strcmp(magic, "REDIS") != 0) {
    printf("Error: Invalid RDB file format\n");
    fclose(file);
    return;
  }
  char version[6];
  fread(version, sizeof(char), 4, file);
  version[5] = '\0';
  printf("Version: %s\n", version);
  int rdbVersion = atoi(version);
  printf("RDB Version: %d\n", rdbVersion);
  // Loop until end of file
  unsigned char opcode;
  uint64_t expireTime = 0;
  while (fread(&opcode, sizeof(opcode), 1, file)) {
    printf("Opcode: %02X\n", opcode);
    switch (opcode) {
    case SELECTDB: {
      uint64_t dbNumber = readLengthEncoding(file);
      printf("Database Selector: %lu\n", dbNumber);
      break;
    }
    case RESIZEDB: {
      uint64_t hashTableSize = readLengthEncoding(file);
      uint64_t expireHashTableSize = readLengthEncoding(file);
      printf("Resizedb: Hash Table Size: %lu, Expire Hash Table Size: %lu",
             hashTableSize,expireHashTableSize);
      for (int i = 0; i < hashTableSize; i++) {
        parse_key_value(file);
      }
      break;
    }
    case AUX: {
      char *key = readString(file);
      char *value = readString(file);
      printf("Auxiliary Field: Key: %s, Value: %s\n", key, value);
      free(key);
      free(value);
      break;
    }
    case EXPIRETIME: {
     expireTime = readLengthEncoding(file);
      printf("Expire Time: %lu seconds\n", expireTime);
      break;
    }
    case EXPIRETIMEMS: {
      expireTime = readLengthEncoding(file);
      printf("Expire Time: %lu milliseconds\n", expireTime);
      break;
    }
    case EOF_OP: {
      printf("End of RDB file\n");
      fclose(file);
      return;
    }
    default:
      continue;
    }
  }
  fclose(file);
}// Function to send a message in RESP format
void send_msg(int sd, const char *msg) {
    char msg_formatted[256];
    snprintf(msg_formatted, sizeof(msg_formatted), "$%ld\r\n%s\r\n", strlen(msg), msg);
    send(sd, msg_formatted, strlen(msg_formatted), 0);
}
uint64_t get_current_time() {
  struct timespec ts;
  if (clock_gettime(CLOCK_REALTIME, &ts) == 0) {
    return (uint64_t)(ts.tv_sec * 1000 + ts.tv_nsec / 1000000);
  } else {
    return 0;
  }
}


void *handle_client(void *client_socket_ptr) {
    int sd = *(int *)client_socket_ptr;
    free(client_socket_ptr);

    char readBuffer[1024];
    while (1) {
        memset(readBuffer, 0, sizeof(readBuffer));  // Reset the buffer before reading
        int valread = read(sd, readBuffer, sizeof(readBuffer) - 1);
        
        if (valread == 0) {
            // Client disconnected
            printf("Client disconnected\n");
            close(sd);
            break;
        } else if (valread < 0) {
            printf("Read error: %s\n", strerror(errno));
            close(sd);
            break;
        } else {
            // Null-terminate the readBuffer for safe string operations
            readBuffer[valread] = '\0';

            // Print the contents of readBuffer
            printf("Received message: %s\n", readBuffer);

            // Parse the command
            char command[16] = {0};
            char subcommand[16] = {0};
            sscanf(readBuffer, "*%*d\n$%*d\n%15s", command);
            sscanf(readBuffer, "*%*d\n$%*d\n%*s\n$%*d\n%15s", subcommand);

            // Convert command to lowercase
            for (int j = 0; command[j]; j++) {
                command[j] = tolower(command[j]);
                 subcommand[j] = tolower(subcommand[j]);
            }

            if (strncmp(command, "echo", 4) == 0) {
                // Parse the argument for the ECHO command
                char argument[256] = {0};
                sscanf(readBuffer, "*%*d\n$%*d\n%*s\n$%*d\n%255s", argument);
                send_msg(sd, argument);
            } 
            else if (strncmp(command, "set", 3) == 0) {
                // Handle SET command with or without expiry
                char key[256] = {0};
                char value[256] = {0};
                int expiry = 0;

                if (strstr(readBuffer, "px") != NULL) {
                    int parsed = sscanf(readBuffer, "*%*d\n$%*d\n%*s\n$%*d\n%255s\n$%*d\n%255s\n$%*d\n%*s\n$%*d\n%d", key, value, &expiry);

                    if (parsed == 3) {
                        redis_map_entry *entry = (redis_map_entry *)malloc(sizeof(redis_map_entry));
                        strcpy(entry->key, key);
                        strcpy(entry->value, value);
                        entry->time_set = get_current_time();
                        entry->expiry = (uint64_t)expiry;
                        redis_map.map[redis_map.size++] = entry;
                        send_msg(sd, "OK");
                    } else {
                        printf("Error parsing the input string.\n");
                    }
                } else {
                    sscanf(readBuffer, "*%*d\n$%*d\n%*s\n$%*d\n%255s\n$%*d\n%255s", key, value);

                    redis_map_entry *entry = (redis_map_entry *)malloc(sizeof(redis_map_entry));
                    strcpy(entry->key, key);
                    strcpy(entry->value, value);
                    entry->time_set = 0;
                    entry->expiry = 0;
                    redis_map.map[redis_map.size++] = entry;
                    send_msg(sd, "OK");
                }
            } 
            else if (strncmp(command, "get", 3) == 0) {
                // Parse the argument for the GET command
                char key[256] = {0};
                sscanf(readBuffer, "*%*d\n$%*d\n%*s\n$%*d\n%255s", key);

                char *value = NULL;
                int found = 0;
                for (int i = 0; i < redis_map.size; i++) {
                    if (strcmp(key, redis_map.map[i]->key) == 0) {
                        uint64_t curr_time = get_current_time();
                        if (redis_map.map[i]->expiry == 0 || (curr_time - redis_map.map[i]->time_set) <= redis_map.map[i]->expiry) {
                            value = redis_map.map[i]->value;
                            send_msg(sd, value);
                        } else {
                            send(sd, "$-1\r\n", strlen("$-1\r\n"), 0); // Return nil for expired key
                        }
                        found = 1;
                        break;
                    }
                }
                if (!found) {
                    send(sd, "$-1\r\n", strlen("$-1\r\n"), 0); // Return nil if key not found
                }
            }
            else if (strncmp(command, "config", 6) == 0) {
                

            
                if (strncmp(subcommand, "get", 3) == 0) {
                    char param[256] = {0};
                    sscanf(readBuffer, "*%*d\n$%*d\n%*s\n$%*d\n%*s\n$%*d\n%255s", param);
            
                    if (strncmp(param, "dir", 3) == 0) {
                        // Respond with the dir parameter
                        char response[512];
                        snprintf(response, sizeof(response), "*2\r\n$3\r\ndir\r\n$%ld\r\n%s\r\n", strlen(rdb_dir), rdb_dir);
                        send(sd, response, strlen(response), 0);
                    } else if (strncmp(param, "dbfilename", 10) == 0) {
                        // Respond with the dbfilename parameter
                        char response[512];
                        snprintf(response, sizeof(response), "*2\r\n$10\r\ndbfilename\r\n$%ld\r\n%s\r\n", strlen(rdb_filename), rdb_filename);
                        send(sd, response, strlen(response), 0);
                    } else {
                        send_msg(sd, "ERR unknown parameter");
                    }
                } else {
                    send_msg(sd, "ERR unknown subcommand");
                }
            }
            else if (strncmp(command, "keys", 4) == 0) {
    // Generate RESP array for all keys
    char response[1024];
    size_t offset = 0;
    offset += snprintf(response + offset, sizeof(response) - offset, "*%zu\r\n", redis_map.size);

    for (size_t i = 0; i < redis_map.size; i++) {
        redis_map_entry *entry = redis_map.map[i];
        size_t key_len = strlen(entry->key);
        offset += snprintf(response + offset, sizeof(response) - offset, "$%zu\r\n%s\r\n", key_len, entry->key);
    }

    send(sd, response, offset, 0);
}else if (strncmp(command, "info", 4) == 0) {
      if (strncmp(subcommand, "replication",11) == 0) {
        if ((strncmp(replication_role, "--replicaof",11)==0)) {
          send_msg(sd,"role:slave");
        } else {
          send_msg(sd,"role:master\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\nmaster_repl_offset:0\n");
        }
        
      }
    }else if (strncmp(command, "replconf",8) == 0) {
      send_msg(sd,"OK");
    }else if (strncmp(command, "psync",5) == 0) {
      send_msg(sd,"FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0");
      send(sd,"$0001\r\n ",strlen("$0001\r\n" ),0);
    }

            else {
                // Handle other commands like PING
                send_msg(sd, "PONG");
            }
        }
    }

    close(sd);
    return NULL;
}
void *listen_to_master(void *sockfd_ptr) {
  int master_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (master_fd == -1) {
    printf("Socket creation to master failed: %s...\n", strerror(errno));
    
  }
  struct sockaddr_in master_addr = {
      .sin_family = AF_INET,
      .sin_port = htons(master_port),
      .sin_addr = {htonl(INADDR_ANY)},
  };
  bind(master_fd, (struct sockaddr *)&master_addr, sizeof(master_addr));
  connect(master_fd, (const struct sockaddr *)&master_addr,sizeof(master_addr));
  send(master_fd, "*1\r\n$4\r\nping\r\n", strlen("*1\r\n$4\r\nping\r\n"), 0);
  
  char readBuffer[1024];
  read(master_fd, readBuffer, sizeof(readBuffer) - 1);
  send(master_fd, "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n", strlen("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n"), 0);
  read(master_fd, readBuffer, sizeof(readBuffer) - 1);
  send(master_fd, "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n", strlen("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"), 0);
  read(master_fd, readBuffer, sizeof(readBuffer) - 1);
  send(master_fd, "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n", strlen("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"), 0);
  
}

        
    
    


int main(int argc, char *argv[]) {
// Disable output buffering
setbuf(stdout, NULL);
// Parse command-line 
for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "--dir") == 0 && i + 1 < argc) {
        strncpy(rdb_dir, argv[++i], sizeof(rdb_dir) - 1);
    }else if (strcmp(argv[i], "--dbfilename") == 0 && i + 1 < argc) {
        strncpy(rdb_filename, argv[++i], sizeof(rdb_filename) - 1);
    }else if (strcmp(argv[i], "--replicaof") == 0 && i + 1 < argc) {
        strncpy(replication_role, argv[i], sizeof(replication_role) - 1);
            slave = true;
    }else if (strcmp(argv[i], "--port") == 0) {
        port = atoi(argv[++i]);
      }
}
redis_map_t entry;
int slave_sockfd = 0;
if (slave) {
pthread_t master_listener_thread;
pthread_create(&master_listener_thread, NULL, listen_to_master, &slave_sockfd);
pthread_detach(master_listener_thread);  // Detach if you don't need to join later
}

// Construct full RDB file path
char rdb_filepath[MAX_ENTRY_STR_SIZE];
snprintf(rdb_filepath, sizeof(rdb_filepath), "%s/%s", rdb_dir, rdb_filename);
parseRDBFile(rdb_filepath);
    

int server_fd, client_addr_len, sd;
struct sockaddr_in client_addr;
    

// Create a socket
server_fd = socket(AF_INET, SOCK_STREAM, 0);
if (server_fd == -1) {
    printf("Socket creation failed: %s...\n", strerror(errno));
    return 1;
}
// Set SO_REUSEADDR to avoid 'Address already in use' errors
int reuse = 1;
if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
    printf("SO_REUSEADDR failed: %s \n", strerror(errno));
    return 1;
}
// Configure server address
struct sockaddr_in serv_addr = {
    .sin_family = AF_INET,
    .sin_port = htons(port),
    .sin_addr = {htonl(INADDR_ANY)},
};

// Bind the socket
if (bind(server_fd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) != 0) {
    printf("Bind failed: %s \n", strerror(errno));
    return 1;
}

// Start listening for connections
int connection_backlog = 5;
if (listen(server_fd, connection_backlog) != 0) {
    printf("Listen failed: %s \n", strerror(errno));
    return 1;
}

printf("Waiting for a client to connect...\n");

while (1) {
    int client_addr_len = sizeof(client_addr);
    int client_socket = accept(server_fd, (struct sockaddr *)&client_addr, &client_addr_len);
    if (client_socket < 0) {
        printf("Accept failed: %s \n", strerror(errno));
        continue;
    }
    else{
        printf("(server is from rahul)Client connected...\n");
    }
    pthread_t thread_id;
    int *client_socket_ptr = malloc(sizeof(int));
    *client_socket_ptr = client_socket;
    pthread_create(&thread_id, NULL, handle_client, client_socket_ptr);
    pthread_detach(thread_id);  // Detach the thread to avoid memory leaks
  }
  close(server_fd);
  return 0;
}

