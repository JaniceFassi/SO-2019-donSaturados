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

	// CONEXIONES
	// KERNEL

	t_log* logger = init_logger();
    t_config* config = read_config();
    t_list* segmentoLista = list_create(); //no se bien como se maneja las commons para los elementos de esta lista


	// Conexion kernel
	u_int16_t kernelServer;
	char * ip = "127.0.0.1";
	u_int16_t port= 7000;

	if(createServer(ip,port,&kernelServer)!=0){
		printf("se me rompio el programa");
		return 1;
	}


	listenForClients(kernelServer,100); // esta funcion esta al pedo
	printf("\nEstoy escuchando\n");


	u_int16_t kernelClient;
	char * p="me llamo Kernel wacho";

	if(acceptConexion(kernelServer,&kernelClient,p,0,0)!=0){
		printf("habiamos llegado tan lejos...");
		return 1;
	}
	printf("lo logramos!!");

	//LISSANDRA
	//la idea aca es hacer el handshake con LFS y recibir el value maximo obvio que no esta terminado

	u_int16_t lfsCliente;
	char * ipLissandra = config_get_string_value(config,"IP_FS");
	u_int16_t puertoLissandra = config_get_string_value(config,"PUERTO_FS");

	int conexionExitosa;

	conexionExitosa = linkClient(&lfsCliente,ipLissandra , puertoLissandra);

	if(conexionExitosa !=0){
		printf("Error al conectarse con LFS"); // no es mejor el perror aca?
	}



/////////////////////////////

	int protocoloFuncion = 0;

	switch(protocoloFuncion){
		case 0:
			//mSelect(char* nombreTabla,u_int16_t keyTabla);
			break;
		case 1:
			// esto me rompia asi que lo comente
			//char* nombreTabla;
			//scanf(String, &nombreTabla);
			//u_int16_t keyTabla;
			//char* valor;
			//mInsert(nombreTabla, keyTabla, valor);
			break;

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


t_config* read_config() {
	return config_create("/home/utnso/tp-2019-1c-donSaturados/MemPool/Mem.config");
}

t_log* init_logger() {
	return log_create("memPrincipal.log", "memPrincipal", 1, LOG_LEVEL_INFO);
}

Segmento *crearSegmento(char* nombre,u_int16_t key){
	Segmento *nuevo = malloc(sizeof(Segmento));
	nuevo->nombre = nombre;
	nuevo->keyTabla = key;
	return nuevo;
}


void mSelect(char* nombreTabla,u_int16_t keyTabla){

}
void mInsert(char* nombreTabla,u_int16_t keyTabla,char* valor){

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

//list_destroy(segmentoLista);
//log_destroy(logger);
//config_destroy(config);



