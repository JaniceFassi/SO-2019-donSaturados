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
t_list* listaDeTablas;
t_log* logger;
t_config *config;


int main(void) {


	//INICIALIZO
	logger = init_logger();
	config = read_config();
	segmentoLista = list_create();
	listaDeTablas = list_create();

	//LISSANDRA
	//la idea aca es hacer el handshake con LFS y recibir el value maximo, obvio que no esta terminado

	u_int16_t lfsCliente;
	char * ipLissandra = config_get_string_value(config,"IP_FS");
	u_int16_t puertoLissandra = config_get_string_value(config,"PUERTO_FS");

	//int valueLissandra = handshakeConLissandra(&lfsCliente,ipLissandra,puertoLissandra);
	//Lo comento por ahora para que no me tire los errores

	//mInsert("TABLA1",57,"DUKI");
	//ignorar esto que son pruebas, todavia parece que no funciona bien el insert
	list_add(segmentoLista,crearSegmento("TABLA1",57,"DUKI"));
	list_add(segmentoLista,crearSegmento("TABLA1",58,"DUKI"));
	list_add(segmentoLista,crearSegmento("TABLA1",59,"DUKI"));

	int index = 0;
	pagina *aux = list_get(segmentoLista,index);


	while(aux){
		printf("Key: %i, Valor: %s \n",aux->keyTabla,aux->valor);
		index++;
		aux = list_get(segmentoLista,index);
	}//prueba para ver que hay en memoria nada mas

/////////////////////////////

	int protocoloFuncion = 0;

	switch(protocoloFuncion){
		case 0:
			//mSelect(char* nombreTabla,u_int16_t keyTabla);
			break;
		case 1:
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

void mSelect(char* nombreTabla,u_int16_t key){
	tabla tablaEncontrada = buscarTabla(nombreTabla);{//busca tabla, tabla = segmento
		if(tablaEncontrada != NULL){
			pagina pag = buscarPagina(tablaEncontrada, key); //busca la pagina

		}else{
			pedirleALissandra(nombreTabla, key);
		}
		if(pag != NULL){
			printf("La tabla %s ha sido encontrada y el valor correspondiente a esa key es: %s \n", nombreTabla, pag->valor);
		}else{
			pedirleALissandra(nombreTabla, key);
		}
	}

}
void mInsert(char* nombreTabla,u_int16_t keyTabla,char* valor){

	if(buscarYreemplazar(keyTabla,valor)){

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

pagina *crearSegmento(char* nombre,u_int16_t key,char* valor){
	pagina *nuevo = malloc(sizeof(pagina));
	nuevo->nombre = nombre;
	nuevo->keyTabla = key;
	nuevo->valor = valor;
	nuevo->modificado = 0;
	nuevo->timestamp = (unsigned)time(NULL);
	return nuevo;
}

int handshakeConLissandra(int lfsCliente,char* ipLissandra,int puertoLissandra){
	int conexionExitosa;
	conexionExitosa = linkClient(&lfsCliente,ipLissandra , puertoLissandra);

		if(conexionExitosa !=0){
			perror("Error al conectarse con LFS");
		}

}

pagina *buscarYreemplazar(u_int16_t keyTablaNueva,char* nuevoValor){

	int tieneMismaKey(pagina *p){
		if((p->keyTabla) == keyTablaNueva){
				p->valor = nuevoValor;
		}
		return ((p->keyTabla) == keyTablaNueva);
	}

	return list_find(segmentoLista,(void*) tieneMismaKey);
}

tabla *buscarTabla(char *unNombre){

	int existeTabla(tabla *unaTabla){
		int encontrado;
		if(strcmp((unaTabla->nombreTabla),unNombre) !=1){
			encontrado = 1;

		}
		return encontrado;
	}

	return list_find(listaDeTablas, (void*) existeTabla);


}

pagina *buscarPagina(tabla unaTabla, u_int16_t unaKey){

	for(int i=0; i<10; i++){
		if(unaTabla ->pag[i]->keyTabla == unaKey){
			return pag[i];
			i = 10;
		}
	}

}

void pedirleALissandra(char *nombreTabla, u_int16_t unaKey){

	char *paquete = empaquetar(1, nombreTabla, unaKey);
	int tamanioPaquete = sizeof(paquete);
	char paqueteAEnviar[1000];
	strcpy(paqueteAEnviar,(string_itoa(tamanioPaquete)));
	strcat(paqueteAEnviar, ";");
	strcat(paqueteAEnviar, &paquete);

	send(lfsCliente, paqueteAEnviar, strlen(paqueteAEnviar), 0); //falta el socket lissandra

}

char *empaquetar(int operacion, char *nombreTabla, u_int16_t keyTabla){
	char *op = string_itoa(operacion);
	char *key = string_itoa(keyTabla);
	char* paquete = string_new();
	string_append(&paquete, &op);
	string_append(&paquete, ";");
	string_append(&paquete, &nombreTabla);
	string_append(&paquete, ";");
	string_append(&paquete, &key);

	return paquete;
}

//list_destroy(segmentoLista);
//list_destroy_and_destroy_elements(listaDeTablas); para que borre la lista y componentes
//log_destroy(logger);
//config_destroy(config);



