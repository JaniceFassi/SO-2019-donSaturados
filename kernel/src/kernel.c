/*
 ============================================================================
 Name        : kernel.c
 Author      : pepe
 Version     :
 Copyright   : Your copyright notice
 Description : Hello World in C, Ansi-style
 ============================================================================
 */

#include "kernel.h"

int main(void) {

	u_int16_t sock;
	char * ip = "127.0.0.1";
	u_int16_t port= 7000;

	int a;

	a=linkClient(&sock,ip , port);

	if(a!=0){
		printf("no se pudo conectar...");
	}

	return EXIT_SUCCESS;
}



