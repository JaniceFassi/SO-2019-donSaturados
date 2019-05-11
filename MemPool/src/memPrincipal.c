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

t_list* segmentoLista;
t_log* logger;
t_config *config;


int main(void) {


	//INICIALIZO
	logger = init_logger();
	config = read_config();
	segmentoLista = list_create();

	//LISSANDRA
	//la idea aca es hacer el handshake con LFS y recibir el value maximo, obvio que no esta terminado

	u_int16_t lfsCliente;
	char * ipLissandra = config_get_string_value(config,"IP_FS");
	u_int16_t puertoLissandra = config_get_string_value(config,"PUERTO_FS");

	//int valueLissandra = handshakeConLissandra(&lfsCliente,ipLissandra,puertoLissandra);
	//Lo comento por ahora para que no me tire los errores

	mInsert("TABLA1",57,"DUKI");


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

//Le robe la consola a SoliJani total es la misma logica
void consola(){

	char* linea;
	while(1){
		//linea = readline(">"); Por que no me reconoce readLine??

		if(!strncmp(linea,"SELECT ",7))
			{

				//selectS();
			}
		if(!strncmp(linea,"INSERT ",7)){
		 		//insert(linea);
		 	}
		if(!strncmp(linea,"CREATE ",7)){
				//create();
			}
		if(!strncmp(linea,"DESCRIBE ",9)){
				//describe();
			}
		if(!strncmp(linea,"DROP ",5)){
				//	drop();
			}

		if(!strncmp(linea,"exit",5)){
				free(linea);
			}
			free(linea);
		}
}

void mSelect(char* nombreTabla,u_int16_t keyTabla){

}
void mInsert(char* nombreTabla,u_int16_t keyTabla,char* valor){

	Segmento *nuevoSegmento = crearSegmento(nombreTabla,keyTabla,valor);

	//Verificar si existe la key en la lista, si existe reemplazar y generar nuevo timestamp (buscar como se genera)

	//Si no existe le cargas el timestamp y luego haces el add

	list_add(segmentoLista,nuevoSegmento);

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

//// Auxiliares

t_config* read_config() {
	return config_create("/home/utnso/tp-2019-1c-donSaturados/MemPool/Mem.config");
}

t_log* init_logger() {
	return log_create("memPrincipal.log", "memPrincipal", 1, LOG_LEVEL_INFO);
}

Segmento *crearSegmento(char* nombre,u_int16_t key,char* value){
	Segmento *nuevo = malloc(sizeof(Segmento));
	nuevo->nombre = nombre;
	nuevo->keyTabla = key;
	nuevo->valor = value;
	nuevo->modificado = 0;
	return nuevo;
}

int handshakeConLissandra(int lfsCliente,char* ipLissandra,int puertoLissandra){
	int conexionExitosa;
	conexionExitosa = linkClient(&lfsCliente,ipLissandra , puertoLissandra);

		if(conexionExitosa !=0){
			perror("Error al conectarse con LFS");
		}

}

//list_destroy(segmentoLista);
//log_destroy(logger);
//config_destroy(config);



