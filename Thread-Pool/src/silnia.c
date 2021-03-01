#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include "cacti.h"

#define ignore (void)

typedef struct col_info col_info_t;

// ID pierwszego aktora
actor_id_t first;

// Liczba którą aktualnie przydzielam aktorowi
int seg_now = 1;

// Role aktorów
role_t role;

// Silnia z której liczby liczę
size_t n;

// Odpowiedź
size_t ans = 1;

// Struktura dla aktora
struct col_info {
    size_t which_seg;
    actor_id_t next;
};

// Ojciec ustawia indeks następnego aktora w kolejności na swojego syna, który
// wysyła mu tą wiadomość
void im_here(void** stateptr, size_t size, void* data) {
    ignore size;

    col_info_t *current_seg = (col_info_t *) *stateptr;
    current_seg->next = (actor_id_t) data;
}

// Dla danego wiersza dostaje liczbę dotychczas policzoną, odczekuje
// podaje wynik do następnego tudzież do wynikowej
// liczby jeżeli jest ostatnim
void count(void** stateptr, size_t size, void* data) {
    ignore data;
    col_info_t *current_seg = (col_info_t *) *stateptr;

    if (current_seg->which_seg == n) {
        ans = size * current_seg->which_seg;
    }
    else {
        message_t msg_count_next = {
                .message_type = 2,
                .nbytes = (long)size * current_seg->which_seg
        };
        send_message(current_seg->next, msg_count_next);
    }

    free(current_seg);
    message_t die_now = {
            .message_type = MSG_GODIE
    };
    send_message(actor_id_self(), die_now);
}

// Alokuje pod stateptr strukture na siebie, jeżeli nie jest pierwszym wysyła
// im_here do ojca, następnie jeżeli nie jest ostatnim to tworzy nowego
// aktora, jeżeli jest to wysyła do pierwszego aktora wiadomośc o rozpoczęcie
// liczenia silni
void hello(void** stateptr, size_t size, void* data) {
    ignore size;
    col_info_t *current_seg = malloc(sizeof(col_info_t));
    current_seg->which_seg = seg_now++;
    current_seg->next = -1;

    *stateptr = current_seg;

    message_t msg_here = {
            .message_type = 1,
            .data = (void *) actor_id_self()
    };

    if (current_seg->which_seg != 0)
        send_message((actor_id_t)data, msg_here);

    if (current_seg->which_seg < n) {
        message_t go_spawn = {
                .message_type = MSG_SPAWN,
                .nbytes = 0,
                .data = &role
        };
        send_message(actor_id_self(), go_spawn);
    }
    else {
        message_t msg_count = {
                .message_type = 2,
                .nbytes = 1
        };
        send_message(first, msg_count);
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

    scanf("%zd", &n);

    if (actor_system_create(&first, &role) < 0)
        abort();

    actor_system_join(first);
    printf("%zu", ans);
    return 0;
}
