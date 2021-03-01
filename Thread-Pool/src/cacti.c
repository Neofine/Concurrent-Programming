#include <pthread.h>
#include <semaphore.h>
#include <stddef.h>
#include <stdbool.h>
#include <stdlib.h>
#include <signal.h>
#include "cacti.h"
#include "err.h"

#define BEGIN_LENGTH 16
#define ERROR -1
#define SUCCESS 1

typedef struct queue queue_t;

typedef struct actor_data actor_data_t;

typedef struct system_data system_data_t;

// Wskaźnik którego używają aktory do używania struktury
// współdzielonej system_data
system_data_t *sys_data;

// Stan wewnętrzny aktora
typedef struct actor_data {
    pthread_mutex_t *actor_lock;

    // zaznaczone na true jeżeli przeprocesował wiadomość godie
    bool ignoring_mess;
    bool is_waiting;
    bool has_stateptr;
    actor_id_t id;
    void **stateptr;

    queue_t *queue;

    role_t *job;
} actor_data_t;

// Struktura z której aktory korzystają jako z globalnej, do komunikacji oraz
// do przekazywania informacji
typedef struct system_data {
    pthread_t **threads;
    pthread_t *signal_thread;
    pthread_attr_t *threads_attr;

    pthread_mutex_t *mutex;
    pthread_cond_t *ping_thread;
    pthread_mutex_t *start_mutex;
    sem_t *thread_initializing_mutex;
    size_t threads_to_initialize;

    queue_t *actors_queue;
    queue_t *actors;

    size_t lazy_actors;
    size_t act_size;
    // ilość aktorów którzy przeprocessowali wiadomość godie oraz
    // nie mają już wiadomości na kolejce
    size_t finished_actors;

    bool sigint_registered;
} system_data_t;


// -------------------------- QUEUE -----------------------------------------

// Struktura kolejki
struct queue {
    void **array;

    size_t size;
    size_t first_elem;
    size_t length;
    size_t max_size;
};

// Tworzy kolejkę i ją zwraca
queue_t* create_queue(size_t max_size) {
    queue_t *queue = malloc(sizeof(queue_t));
    if (queue == NULL) {
        return NULL;
    }

    queue->array = malloc(sizeof(void *) * BEGIN_LENGTH);

    if (queue->array == NULL) {
        return NULL;
    }

    queue->max_size = max_size;
    queue->size = BEGIN_LENGTH;
    queue->first_elem = 0;
    queue->length = 0;

    return queue;
}

// Zwiększa rozmiar tablicy kolejki, ERROR jak już jest maksymalny możliwy
int increase_array(queue_t *queue) {
    if (queue->size == queue->max_size)
        return ERROR;

    size_t new_size;
    if (queue->size * 2 >= queue->max_size)
        new_size = queue->max_size;
    else new_size = queue->size * 2;

    void **helper;
    helper = malloc(sizeof(void *) * new_size);

    if (helper == NULL) {
        return ERROR;
    }

    size_t idx = queue->first_elem;
    for (size_t i = 0; i < queue->size; i++) {
        helper[i] = queue->array[idx];
        idx = (idx + 1) % queue->size;
    }

    free(queue->array);

    queue->array = helper;
    queue->first_elem = 0;
    queue->size = new_size;
    return SUCCESS;
}

// Zwraca true jeżeli z sukcesem element dodał się na kolejkę, false wpp
int append_queue(queue_t *queue, void *who) {
    if (queue->size == queue->length && increase_array(queue) == ERROR) {
        return ERROR;
    }

    size_t next_elem = (queue->first_elem + queue->length) % queue->size;

    queue->array[next_elem] = who;
    queue->length++;

    return SUCCESS;
}

// Bierze pierwszy element z kolejki
void *get_first(queue_t *queue) {
    void *answer = queue->array[queue->first_elem];

    queue->first_elem = (queue->first_elem + 1) % queue->size;
    queue->length--;

    return answer;
}

// Zwraca wartość pod indeksem kolejki lub NULL jeżeli nie poprawny
// argument został podany
void *at_idx(queue_t *queue, actor_id_t idx) {
    if ((actor_id_t) queue->length <= idx || idx < 0) {
        return NULL;
    }

    return queue->array[idx];
}

// Zwraca rozmiar kolejki
int queue_size(queue_t *queue) {
    return queue->length;
}

// Oczyszcza kolejkę
void empty_queue(queue_t *queue) {
    free(queue->array);
    free(queue);
}

// ------------------------------- sys_data -------------------------------------

// Demallocuje system
void delete_system() {
    if (sys_data == NULL) {
        return;
    }

    if (sys_data->mutex != NULL && pthread_mutex_destroy(sys_data->mutex) != 0) {
        syserr("Could not destroy mutex.");
    }
    free(sys_data->mutex);

    if (sys_data->start_mutex != NULL &&
        pthread_mutex_destroy(sys_data->start_mutex) != 0) {
        syserr("Could not destroy mutex.");
    }
    free(sys_data->start_mutex);

    if (sys_data->thread_initializing_mutex != NULL
        && sem_destroy(sys_data->thread_initializing_mutex) != 0) {
        syserr("Could not destroy semaphore.");
    }
    free(sys_data->thread_initializing_mutex);

    if (sys_data->ping_thread != NULL
        && pthread_cond_destroy(sys_data->ping_thread) != 0) {
        syserr("Could not destroy conditional.");
    }
    free(sys_data->ping_thread);

    if (sys_data->threads_attr != NULL
        && pthread_attr_destroy(sys_data->threads_attr) != 0) {
        syserr("Could not destroy thread attribute.");
    }
    free(sys_data->threads_attr);

    empty_queue(sys_data->actors_queue);

    // dealokuje kolejki wiadomości aktorów
    for (size_t i = 0; i < sys_data->act_size; i++) {
        actor_data_t *actor_now = (actor_data_t *)at_idx(sys_data->actors, i);
        empty_queue(actor_now->queue);
        if (actor_now->actor_lock != NULL
            && pthread_mutex_destroy(actor_now->actor_lock) != 0) {
            syserr("Could not destroy thread attribute.");
        }
        free(actor_now->actor_lock);

        if (actor_now->has_stateptr)
            free(actor_now->stateptr);

        free(actor_now);
    }
    empty_queue(sys_data->actors);

    for (size_t i = 0; i < POOL_SIZE; i++) {
        free(sys_data->threads[i]);
    }
    free(sys_data->threads);
}

// Mallocuje system
int malloc_system() {
    sys_data->mutex = malloc(sizeof(pthread_mutex_t));
    if (sys_data->mutex == NULL ||
        pthread_mutex_init(sys_data->mutex, 0) != 0) {
        delete_system();
        return ERROR;
    }

    sys_data->start_mutex = malloc(sizeof(pthread_mutex_t));
    if (sys_data->start_mutex == NULL
        || pthread_mutex_init(sys_data->start_mutex, 0) != 0) {
        delete_system();
        return ERROR;
    }

    sys_data->thread_initializing_mutex = malloc(sizeof(sem_t));
    if (sys_data->thread_initializing_mutex == NULL
        || sem_init(sys_data->thread_initializing_mutex, 0, 0) != 0) {
        delete_system();
        return ERROR;
    }

    sys_data->ping_thread = malloc(sizeof(pthread_cond_t));
    if (sys_data->ping_thread == NULL
        || pthread_cond_init(sys_data->ping_thread, 0) != 0) {
        delete_system();
        return ERROR;
    }

    sys_data->actors_queue = create_queue(CAST_LIMIT);
    if (sys_data->actors_queue == NULL) {
        delete_system();
        return ERROR;
    }

    sys_data->actors = create_queue(CAST_LIMIT);
    if (sys_data->actors == NULL) {
        delete_system();
        return ERROR;
    }

    sys_data->threads_attr = malloc(sizeof(pthread_attr_t));
    if (sys_data->threads_attr == NULL ||
        pthread_attr_init(sys_data->threads_attr)!= 0) {
        delete_system();
        return ERROR;
    }

    if (pthread_attr_setdetachstate(sys_data->threads_attr,
                                    PTHREAD_CREATE_JOINABLE) != 0) {
        delete_system();
        return ERROR;
    }

    sys_data->threads = malloc(sizeof (pthread_t) * POOL_SIZE);
    if (sys_data->threads == NULL) {
        delete_system();
        return ERROR;
    }

    sys_data->signal_thread = malloc(sizeof(pthread_t));
    if (sys_data->signal_thread == NULL) {
        delete_system();
        return ERROR;
    }

    return SUCCESS;
}

void *worker_thread();
void *watching_thread();

// Inicjuje system
int init_system() {
    if (!malloc_system())
        return ERROR;

    if (pthread_mutex_lock(sys_data->mutex) != 0) {
        syserr("Could not lock mutex.");
    }

    sys_data->act_size = 1;
    sys_data->finished_actors = 0;
    sys_data->lazy_actors = 1;
    sys_data->sigint_registered = false;

    // ilość inicjowanych wątków jest równa POOL_SIZE + 1, 1 służy nasłuchiwaniu
    // sygnału SIGINT, natomiast reszta obsługuje aktorów
    sys_data->threads_to_initialize = POOL_SIZE + 1;

    for (size_t i = 0; i < POOL_SIZE; i++) {
        sys_data->threads[i] = malloc(sizeof(pthread_t));

        if (sys_data->threads[i] == NULL ||
            pthread_create(sys_data->threads[i], sys_data->threads_attr,
                           worker_thread, NULL) != 0) {
            syserr("Cannot create thread.");
        }
    }

    if (pthread_create(sys_data->signal_thread, sys_data->threads_attr,
                       watching_thread, NULL) != 0) {
        syserr("Cannot create thread.");
    }

    if (sem_wait(sys_data->thread_initializing_mutex) != 0) {
        syserr("Error during waiting on semaphore.");
    }

    if (pthread_mutex_unlock(sys_data->mutex) != 0) {
        syserr("Could not unlock mutex.");
    }
    return SUCCESS;
}

void join_threads() {
    if (pthread_join(*(sys_data->signal_thread), 0) != 0) {
        syserr("Could not join threads.");
    }
}

void actor_system_join(actor_id_t actor) {
    if (sys_data == NULL)
        return;
    actor_data_t *actor_data = at_idx(sys_data->actors, actor);

    if (actor_data != NULL) {
        join_threads();
    }
}

// Inicjuje aktora, zwraca -1 ja się nie udało 1 jak się udało
int init_actor(actor_data_t *actor, role_t *const role) {
    actor->ignoring_mess = false;
    actor->is_waiting = false;
    actor->queue = create_queue(ACTOR_QUEUE_LIMIT);
    actor->job = role;
    actor->has_stateptr = false;

    actor->actor_lock = malloc(sizeof(pthread_mutex_t));
    if (actor->actor_lock == NULL
        || pthread_mutex_init(actor->actor_lock, 0) != 0) {
        syserr("Failed to create mutex.");
    }

    actor->id = 0;
    return append_queue(sys_data->actors, actor);
}

int actor_system_create(actor_id_t *actor, role_t *const role) {
    if (sys_data != NULL)
        return -1;

    sys_data = malloc(sizeof(system_data_t));
    if (sys_data == NULL) {
        free(sys_data);
        free(sys_data->signal_thread);

        sys_data = NULL;
        return -1;
    }
    if (init_system() == ERROR || sys_data == NULL) {
        free(sys_data->signal_thread);
        free(sys_data);

        sys_data = NULL;
        return -1;
    }

    actor_data_t *new_actor = malloc(sizeof(actor_data_t));
    if (new_actor == NULL) {
        delete_system();
        free(sys_data->signal_thread);
        free(sys_data);

        sys_data = NULL;
        return -1;
    }

    if (init_actor(new_actor, role) == ERROR) {
        delete_system();
        free(sys_data->signal_thread);
        free(sys_data);

        sys_data = NULL;
        return -1;
    }
    *actor = new_actor->id;

    message_t to_send;
    to_send.message_type = MSG_HELLO;
    to_send.nbytes = 0;
    to_send.data = NULL;
    send_message(new_actor->id, to_send);

    return 0;
}

_Thread_local actor_id_t thread_local_id;

actor_id_t actor_id_self() {
    return thread_local_id;
}

void set_actor_id(actor_id_t new_id) {
    thread_local_id = new_id;
}

int send_message(actor_id_t actor, message_t message) {
    if (sys_data == NULL) {
        return -5;
    }

    if (pthread_mutex_lock(sys_data->mutex) != 0) {
        syserr("Could not lock mutex.");
    }
    actor_data_t *actor_data = at_idx(sys_data->actors, actor);
    if (pthread_mutex_unlock(sys_data->mutex) != 0) {
        syserr("Could not unlock mutex.");
    }

    if (actor_data != NULL) {
        if (pthread_mutex_lock(actor_data->actor_lock) != 0) {
            syserr("Could not lock mutex.");
        }

        // jeżeli aktor otrzymał komendę godie lub sys_data dostał siginta
        // to niepowodzenie, aktor nie może przyjąć tej wiadomości
        if (actor_data->ignoring_mess || sys_data->sigint_registered) {
            if (pthread_mutex_unlock(actor_data->actor_lock) != 0) {
                syserr("Could not unlock mutex.");
            }
            return -1;
        }


        message_t *tmp_message = malloc(sizeof(message_t));
        if (tmp_message != NULL) {
            *tmp_message = message;
            if (append_queue(actor_data->queue, tmp_message) == ERROR) {
                if (pthread_mutex_unlock(actor_data->actor_lock) != 0) {
                    syserr("Could not unlock mutex.");
                }
                free(tmp_message);
                // Nie udało się dodać na kolejkę, nieznany
                // (z punktu widzenia treści) błąd
                return -3;
            }


            if (pthread_mutex_lock(sys_data->mutex) != 0) {
                syserr("Could not lock mutex.");
            }

            if (queue_size(actor_data->queue) == 1 && actor_data->is_waiting == false) {
                actor_data->is_waiting = true;
                sys_data->lazy_actors--;
                append_queue(sys_data->actors_queue, &actor_data->id);
                pthread_cond_signal(sys_data->ping_thread);
            }

            if (pthread_mutex_unlock(sys_data->mutex) != 0) {
                syserr("Could not unlock mutex.");
            }

            if (pthread_mutex_unlock(actor_data->actor_lock) != 0) {
                syserr("Could not unlock mutex.");
            }
        }
        else {
            if (pthread_mutex_unlock(actor_data->actor_lock) != 0) {
                syserr("Could not unlock mutex.");
            }
            free(tmp_message);
            // return -4 gdyż nie udało nam się zaalokować pamięci na wiadomość
            // może być obojętny błąd związany z niepowodzeniem
            return -4;
        }
    }
    else
        return -2;
    return 0;
}

// Procesuje wiadomości aktora
void process_messages(actor_data_t *actor) {
    queue_t *messages = actor->queue;

    if (pthread_mutex_lock(actor->actor_lock) != 0) {
        syserr("Could not lock mutex.");
    }
    size_t messages_left = queue_size(messages);
    if (pthread_mutex_unlock(actor->actor_lock) != 0) {
        syserr("Could not unlock mutex.");
    }

    for (size_t i = 1; i <= messages_left; i++) {
        if (pthread_mutex_lock(actor->actor_lock) != 0) {
            syserr("Could not lock mutex.");
        }
        message_t *current_mess = get_first(messages);
        if (pthread_mutex_unlock(actor->actor_lock) != 0) {
            syserr("Could not unlock mutex.");
        }

        if (current_mess->message_type == MSG_SPAWN &&
            !sys_data->sigint_registered) {
            if (pthread_mutex_lock(sys_data->mutex) != 0) {
                syserr("Could not lock mutex.");
            }

            if (sys_data->act_size >= CAST_LIMIT) {
                if (pthread_mutex_unlock(sys_data->mutex) != 0) {
                    syserr("Could not unlock mutex.");
                }
                free(current_mess);
                continue;
            }

            actor_data_t *new_actor = malloc(sizeof(actor_data_t));
            if (new_actor == NULL) {
                free(current_mess);
                continue;
            }

            new_actor->ignoring_mess = false;
            new_actor->is_waiting = false;
            new_actor->queue = create_queue(ACTOR_QUEUE_LIMIT);
            new_actor->job = current_mess->data;
            new_actor->has_stateptr = false;

            new_actor->actor_lock = malloc(sizeof(pthread_mutex_t));
            if (new_actor->actor_lock == NULL
                || pthread_mutex_init(new_actor->actor_lock, 0) != 0) {
                syserr("Failed to create mutex.");
            }

            new_actor->id = sys_data->act_size++;
            sys_data->lazy_actors++;
            if (append_queue(sys_data->actors, new_actor) == ERROR) {
                free(new_actor);
                free(current_mess);
                continue;
            }

            if (pthread_mutex_unlock(sys_data->mutex) != 0) {
                syserr("Could not unlock mutex.");
            }

            message_t to_send;
            to_send.message_type = MSG_HELLO;
            to_send.nbytes = sizeof(void *);
            to_send.data = (void *) actor->id;
            send_message(new_actor->id, to_send);
        }
        else if (current_mess->message_type == MSG_GODIE) {
            if (pthread_mutex_lock(actor->actor_lock) != 0) {
                syserr("Could not lock mutex.");
            }

            actor->ignoring_mess = true;

            if (pthread_mutex_unlock(actor->actor_lock) != 0) {
                syserr("Could not unlock mutex.");
            }
        }
        else if (current_mess->message_type != MSG_GODIE &&
                 current_mess->message_type != MSG_SPAWN){
            if (current_mess->message_type == MSG_HELLO) {
                if (!actor->has_stateptr)
                    actor->stateptr = malloc(sizeof(void*));

                if (actor->stateptr == NULL) {
                    free(current_mess);
                    continue;
                }

                actor->has_stateptr = true;
                *actor->stateptr = NULL;
            }

            if (current_mess->message_type > (long) actor->job->nprompts - 1)
                syserr("Cannot process this message.");
            actor->job->prompts[current_mess->message_type](actor->stateptr,
                                current_mess->nbytes, current_mess->data);
        }
        free(current_mess);
    }

    if (pthread_mutex_lock(actor->actor_lock) != 0) {
        syserr("Could not lock mutex.");
    }

    if (pthread_mutex_lock(sys_data->mutex) != 0) {
        syserr("Could not lock mutex.");
    }

    if (messages->length == 0 && (actor->ignoring_mess || sys_data->sigint_registered)) {
        sys_data->finished_actors++;
    }
    else if (messages->length != 0) {
        actor->is_waiting = true;
        append_queue(sys_data->actors_queue, &actor->id);
    }
    else {
        sys_data->lazy_actors++;
        actor->is_waiting = false;
    }

    if (pthread_mutex_unlock(sys_data->mutex) != 0) {
        syserr("Could not unlock mutex.");
    }

    if (pthread_mutex_unlock(actor->actor_lock) != 0) {
        syserr("Could not unlock mutex.");
    }
}

// Funkcja wątku, w while'u bierze aktorów oraz ich obsługuje
void *worker_thread() {
    if (pthread_mutex_lock(sys_data->start_mutex) != 0) {
        syserr("Could not lock mutex.");
    }

    sys_data->threads_to_initialize--;

    sigset_t block_mask;
    sigemptyset(&block_mask);
    sigaddset(&block_mask, SIGINT);
    if (sigprocmask(SIG_BLOCK, &block_mask, 0) == -1)
        syserr("Could not initialize proc mask.");

    if (sys_data->threads_to_initialize == 0
        && sem_post(sys_data->thread_initializing_mutex) != 0) {
        syserr("Could not post semaphore.");
    }

    if (pthread_mutex_unlock(sys_data->start_mutex) != 0) {
        syserr("Could not unlock mutex.");
    }

    while (1) {
        if (pthread_mutex_lock(sys_data->mutex) != 0) {
            syserr("Could not lock mutex.");
        }

        while (sys_data->act_size != sys_data->finished_actors && queue_size(sys_data->actors_queue) == 0) {
            if (pthread_cond_wait(sys_data->ping_thread, sys_data->mutex) != 0) {
                syserr("Error while waiting on condition.");
            }
        }

        if (sys_data->act_size == sys_data->finished_actors) {
            if (pthread_mutex_unlock(sys_data->mutex) != 0) {
                syserr("Could not unlock mutex.");
            }
            return NULL;
        }

        actor_id_t *to_process = get_first(sys_data->actors_queue);

        set_actor_id(*to_process);

        actor_data_t *actor_now = at_idx(sys_data->actors, *to_process);

        if (pthread_mutex_unlock(sys_data->mutex) != 0) {
            syserr("Could not unlock mutex.");
        }

        process_messages(actor_now);

        // mogę to poza mutexem robić gdyż wiem że jestem ostatnim "żyjącym"
        // aktorem
        if (sys_data->finished_actors == sys_data->act_size) {
            if (pthread_cond_broadcast(sys_data->ping_thread) != 0) {
                syserr("Could not broadcast conditional.");
            }
            return NULL;
        }
    }
}

// Zachodzi jak sigint został wywołany na program
void catch () {
    if (pthread_mutex_lock(sys_data->mutex) != 0) {
        syserr("Could not unlock mutex.");
    }
    if (sys_data->sigint_registered)
        return;

    sys_data->sigint_registered = true;

    sys_data->finished_actors = sys_data->lazy_actors;

    if (sys_data->finished_actors == sys_data->act_size) {
        if (pthread_mutex_unlock(sys_data->mutex) != 0) {
            syserr("Could not unlock mutex.");
        }
        pthread_cond_broadcast(sys_data->ping_thread);
    }

    if (pthread_mutex_unlock(sys_data->mutex) != 0) {
        syserr("Could not unlock mutex.");
    }
}

// Wątek który będzie obsługiwał siginta, jak się kończy to usuwa system
void *watching_thread() {
    if (pthread_mutex_lock(sys_data->start_mutex) != 0) {
        syserr("Could not lock mutex.");
    }

    sys_data->threads_to_initialize--;

    struct sigaction action;
    sigset_t block_mask;

    sigemptyset(&block_mask);

    sigaddset(&block_mask, SIGINT);

    action.sa_sigaction = catch;
    action.sa_mask = block_mask;
    action.sa_flags = SA_SIGINFO;

    if (sigaction (SIGINT, &action, 0) == -1)
        syserr("Could not set sigaction.");

    if (sigprocmask(SIG_BLOCK, &block_mask, 0) == -1)
        syserr("Could not set sigaction.");

    if (sys_data->threads_to_initialize == 0
        && sem_post(sys_data->thread_initializing_mutex) != 0) {
        syserr("Could not post semaphore.");
    }

    if (pthread_mutex_unlock(sys_data->start_mutex) != 0) {
        syserr("Could not unlock mutex.");
    }

    for (int i = 0; i < POOL_SIZE; i++) {
        if (pthread_join(*(sys_data->threads[i]), 0) != 0) {
            syserr("Could not join threads.");
        }
    }
    delete_system();
    free(sys_data->signal_thread);
    free(sys_data);

    sys_data = NULL;
    return NULL;
}
