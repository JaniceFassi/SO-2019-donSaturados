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

Lista *primero = NULL;
Lista *ultimo = NULL;

int main(void) {

	// CONEXIONES
	//KERNEL

	u_int16_t kernelServer;
	char * ipKernel = "127.0.0.1";
	u_int16_t puertoKernel= 7000;

	if(createServer(ipKernel,puertoKernel,&kernelServer)!=0){
		printf("se me rompio el programa");
		return 1;
	}



	listenForClients(kernelServer,100);
	printf("\nEstoy escuchando\n");

	u_int16_t kernelClient;
	char * p="me llamo Kernel wacho";

	if(acceptConexion(kernelServer,&kernelClient,p,0,0)!=0){
		printf("habiamos llegado tan lejos...");
		return 1;
	}
	printf("lo logramos!!");


	//LISSANDRA
	u_int16_t lfsCliente;
	char * ipLissandra = "127.0.0.1";
	u_int16_t puertoLissandra= 7000;

	int conexionExitosa;

	conexionExitosa =linkClient(&lfsCliente,ipLissandra , puertoLissandra);

	if(conexionExitosa !=0){
		printf("Error al conectarse con LFS");
	}


/////////////////////////////


	int protocoloFuncion = 0;

	switch(protocoloFuncion){
		case 0:
			mSelect();
			break;
		case 1:
			char* nombreTabla;
			scanf(String, &nombreTabla);
			u_int16_t keyTabla;
			char* value;

			mInsert(nombreTabla, keyTabla, value);
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

void mSelect(){

}
void mInsert(char* nombreTabla, u_int16_t keyTabla, char* valor){


}
void mCreate(){

}
void mDescribe(){

}
void mDrop(){

}
void mJournal(){

}
void mGossip(){

}





