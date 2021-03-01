/*
 * POCHODZENIE:
 * https://moodle.mimuw.edu.pl/mod/folder/view.php?id=30299
 * (Laboratorium numer 9, przykłady)
 */
#ifndef CACTI_ERR_H
#define CACTI_ERR_H

/* wypisuje informacje o blednym zakonczeniu funkcji systemowej
i konczy dzialanie */
extern void syserr(const char *fmt, ...);

/* wypisuje informacje o bledzie i konczy dzialanie */
extern void fatal(const char *fmt, ...);

#endif //CACTI_ERR_H
