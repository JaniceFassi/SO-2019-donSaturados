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
u_int16_t tamanioMaximo;

int main(void) {


	//INICIALIZO
	logger = init_logger();
	config = read_config();
	segmentoLista = list_create(); //Va a ser nuestra solucion pedorra de memoria por el momento, un unico segmento con  una sola pagina.

	//LISSANDRA
	//la idea aca es hacer el handshake con LFS y recibir el value maximo, obvio que no esta terminado

	u_int16_t lfsCliente;
	char * ipLissandra = config_get_string_value(config,"IP_FS");
	u_int16_t puertoLissandra = config_get_string_value(config,"PUERTO_FS");

	//handshakeConLissandra(&lfsCliente,ipLissandra,puertoLissandra);
	//Lo comento por ahora para que no me tire los errores

	mInsert("TABLA1",57,"DUKI");
	mInsert("TABLA1",57,"DUKI2");
	mInsert("TABLA2",59,"Esto funciona");
	mInsert("TABLA2",566,"Esto funciona2");

	verMemoria();


/////////////////////////////

	int protocoloFuncion = 0;

	switch(protocoloFuncion){
		case 0:
			//mSelect(char* nombreTabla,u_int16_t keyTabla);
			break;
		case 1:
			//mInsert()
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

	if(buscarYreemplazar(keyTabla,valor) == NULL){

		list_add(segmentoLista,crearSegmento(nombreTabla,keyTabla,valor));
	}
	//Explicacion:
	//se fija si existe la tabla, en ese caso se fija si ya hay alguien con esa key, si hay alguien lo reemplaza y sino agrega la pagina (not done yet)
	//si no existe la tabla, la crea y la agrega
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

segmento *crearSegmento(char* nombre,u_int16_t key,char* valor){
	segmento *nuevo = malloc(sizeof(segmento));
	nuevo->nombre = nombre;
	nuevo->keyTabla = key;
	nuevo->valor = valor;
	nuevo->modificado = 0;
	nuevo->timestamp = (unsigned)time(NULL);
	return nuevo;
}

void handshakeConLissandra(u_int16_t lfsCliente,char* ipLissandra,u_int16_t puertoLissandra){
	int conexionExitosa;
	int id; //para que no rompa pero ni idea de donde saco esta vaina
	conexionExitosa = linkClient(&lfsCliente,ipLissandra , puertoLissandra,id);

		if(conexionExitosa !=0){
			perror("Error al conectarse con LFS");
		}

		recvData(lfsCliente, &tamanioMaximo, sizeof(u_int16_t));
}


void *buscarYreemplazar(u_int16_t keyTablaNueva,char* nuevoValor){

	int tieneMismaKey(segmento *p){
		if((p->keyTabla) == keyTablaNueva){
				p->valor = nuevoValor;
				p->timestamp = (unsigned)time(NULL);
		}
		return ((p->keyTabla) == keyTablaNueva);
	}

	return list_find(segmentoLista,(void*) tieneMismaKey);
}

void verMemoria(){
	int index = 0;
	segmento *aux = list_get(segmentoLista,index);

		while(aux){
			printf("Key: %i, Valor: %s TimeStamp: %d \n",aux->keyTabla,aux->valor,aux->timestamp);
			index++;
			aux = list_get(segmentoLista,index);
		}//prueba para ver que hay en memoria nada mas

		if(!aux){
			printf("La memoria esta vacia o se termino de mostrar \n");
		}
}

//list_destroy(segmentoLista);
//log_destroy(logger);
//config_destroy(config);



