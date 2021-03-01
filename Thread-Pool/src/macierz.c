#include <zlib.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include "cacti.h"

#define ignore (void)

typedef struct point point_t;
typedef struct col_info col_info_t;

// Macierz na której pracuje program
point_t **matrix;

// ID pierwszego aktora
actor_id_t first;

// Numer kolumny którą teraz przydzielam aktorowi
int col_here = 0;

// Rozmiary macierzy
size_t k, n;

// Role aktorów
role_t role;

// Tablica wynikowa
int *row_sum;

// Struktura na punkt w macierzy
struct point {
    int value;
    uint32_t time;
};

// Struktura dla aktora
struct col_info {
    size_t col_num;
    size_t counter;
    actor_id_t next;
};

// Ojciec ustawia indeks następnego aktora w kolejności na swojego syna, który
// wysyła mu tą wiadomość
void im_here(void** stateptr, size_t size, void* data) {
    ignore size;
    col_info_t *col_now = (col_info_t *) *stateptr;
    col_now->next = (actor_id_t) data;
}

// Dla danego wiersza dostaje sumę dotychczas policzoną, odczekuje
// pewną ilość milisekund oraz podaje wynik do następnego tudzież do wynikowej
// tablicy jeżeli jest ostatnim
void count(void** stateptr, size_t size, void* data) {
    col_info_t *col_now = (col_info_t *) *stateptr;
    point_t point_now = matrix[size][col_now->col_num];
    usleep(point_now.time * 1000);
    if (col_now->col_num == n - 1) {
        row_sum[size] = (long) (point_now.value + data);
    }
    else {
        message_t msg_count_next = {
                .message_type = 2,
                .nbytes = size,
                .data = point_now.value + data
        };
        send_message(col_now->next, msg_count_next);
    }

    col_now->counter++;
    if (col_now->counter == k) {
        free(col_now);
        message_t die_now = {
                .message_type = MSG_GODIE
        };
        send_message(actor_id_self(), die_now);
    }
}

// Alokuje pod stateptr strukture na siebie, jeżeli nie jest pierwszym wysyła
// im_here do ojca, następnie jeżeli nie jest ostatnim to tworzy nowego
// aktora, jeżeli jest to wysyła do pierwszego aktora wiadomości o rozpoczęcie
// liczenia danego wiersza
void hello(void** stateptr, size_t size, void* data) {
    ignore size;
    col_info_t *col_now = malloc(sizeof(col_info_t));
    col_now->col_num = col_here++;
    col_now->counter = 0;
    col_now->next = -1;

    *stateptr = col_now;

    message_t msg_here = {
            .message_type = 1,
            .data = (void *) actor_id_self()
    };

    if (col_now->col_num != 0)
       send_message((actor_id_t)data, msg_here);

    if (col_now->col_num < n - 1) {
        message_t go_spawn = {
                .message_type = MSG_SPAWN,
                .nbytes = 0,
                .data = &role
        };
        send_message(actor_id_self(), go_spawn);
    }
    else {
        for (size_t i = 0; i < k; i++) {
            message_t msg_count = {
                    .message_type = 2,
                    .nbytes = i,
                    .data = 0
            };
            send_message(first, msg_count);
        }
    }
}


int main() {
    const size_t nprompts = 3;
    void (**prompts)(void**, size_t, void*) = malloc(sizeof(void*) * nprompts);
    prompts[0] = &hello;
    prompts[1] = &im_here;
    prompts[2] = &count;

    role.nprompts = nprompts;
    role.prompts = prompts;

    scanf("%zd%zd", &k, &n);

    size_t idx_amount = k * n;
    row_sum = calloc(k, sizeof(int));

    matrix = malloc(sizeof(point_t) * k);
    for (size_t i = 0; i < k; i++)
        matrix[i] = malloc(sizeof(point_t) * n);

    for (size_t i = 0; i < idx_amount; i++) {
        int v, t;
        scanf("%d%d", &v, &t);
        size_t row = i / n;
        size_t col = i % n;
        point_t new_point;
        new_point.value = v;
        new_point.time = t;
        matrix[row][col] = new_point;
    }

    if (actor_system_create(&first, &role) < 0)
        abort();

    actor_system_join(first);
    for (size_t i = 0; i < k; i++) {
        printf("%d\n", row_sum[i]);
    }

    free(row_sum);
    for (size_t i = 0; i < k; i++)
        free(matrix[i]);
    free(matrix);
    return 0;
}
