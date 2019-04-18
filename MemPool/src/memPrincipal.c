/*
 ============================================================================
 Name        : memPrincipal.c
 Author      : mpelozzi
 Version     :
 Copyright   : Your copyright notice
 Description : Hello World in C, Ansi-style
 ============================================================================
 */


#include "memPrincipal.h"



int main(void) {

	int protocoloFuncion;

	switch(protocoloFuncion){
		case 0:
			mSelect();
			break;
		case 1:
			mInsert();
			break;
		case 2:
			mCreate();
			break;
		case 3:
			mDescribe();
			break;
		case 4:
			mDrop();
			break;
		case 5:
			mJournal();
			break;


	}



	return EXIT_SUCCESS;
}



