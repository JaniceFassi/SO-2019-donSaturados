/*
 ============================================================================
 Name        : FileSystem.c
 Author      : jani_sol
 Version     :
 Copyright   : Your copyright notice
 Description : Hello World in C, Ansi-style
 ============================================================================
 */

#include "FileSystem.h"

int main(void) {
	t_log* logger = init_logger();
		log_info(logger, "Soy FileSystem");
		t_config* config = read_config();

		return EXIT_SUCCESS;
}
t_log* init_logger() {
	return log_create("FileSystem.log", "FileSystem", 1, LOG_LEVEL_INFO);
}
t_config* read_config() {
	return config_create("/home/utnso/tp-2019-1c-donSaturados/LFS/LFS.config");
}
