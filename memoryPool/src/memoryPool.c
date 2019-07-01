/*
 ============================================================================
 Name        : memoryPool.c
 Author      : mpelozzi
 Version     :
 Copyright   : Your copyright notice
 Description : Hello World in C, Ansi-style
 ============================================================================
 */

#include "memoryPool.h"

void* recibirOperacion(void * arg){
	int cli = *(int*) arg;

	char *buffer = malloc(15);
	log_info(logger,"sock: %i",cli);
	//int b= recv(cli, buffer, 2, NULL);
	int b = recvData(cli, buffer, 14);
	//buffer[b]= "\0";
	log_info(logger, "Bytes recibidos: %d", b);
	log_info(logger, "Buffer %s", buffer);
	//1byteop
	//3bytes tamanioop
	//linea con ;

	//int operacion = atoi(buffer);
	//printf("Operacion %s \n", buffer);

/*	char** desempaquetado;
	if(operacion !=5 && operacion !=6){
		char* tamanioPaq = malloc(sizeof(char)*4);

		recv(cli,tamanioPaq, sizeof(char)*3, NULL);
		printf("Tamanio paquete %s \n", tamanioPaq);

		int tamanio = atoi(tamanioPaq);
		char* paquete = malloc(tamanio+1);

		recv(cli, paquete, tamanio, NULL);
		printf("Paquete %s \n", paquete);


		desempaquetado = string_n_split(paquete, 5, ";");

	}
	char* nombreTabla;
	u_int16_t key;
	char* value;
	long timestamp;
	char* consistencia;
	int particiones;
	long tiempoCompactacion;


	switch (operacion) {
				case 0:
					nombreTabla = desempaquetado[0];
					key = atoi(desempaquetado[1]);
					mSelect(nombreTabla, key);
					break;

				case 1:
					//acá hay que ver porque lfs sí manda el timestamp pero kernel no
					//quizás con el id de cliente?
					nombreTabla = desempaquetado[0];
					//timestamp = atol(desempaquetado[1]);
					key = atoi(desempaquetado[1]);
					value = desempaquetado[2];
					//ver si el valor es mayor al maximo, entonces rechaar el insert
					mInsert(nombreTabla, key, value);
					break;

				case 2:
					nombreTabla = desempaquetado[0];
					consistencia = desempaquetado[1];
					particiones = atoi(desempaquetado[2]);
					tiempoCompactacion = atol(desempaquetado[3]);
					//mCreate(nombreTabla, consistencia, particiones, tiempoCompactacion);

					printf("create\n");
				break;

				case 3:
					nombreTabla = desempaquetado[0];
					//mDescribe(nombreTabla);
					printf("describe\n");
					break;

				case 4:
					nombreTabla = desempaquetado[0];
					mDrop(nombreTabla);
					break;

				case 5:
					mJournal();
					break;

				case 6:
					//mGossip();
					printf("gossip\n");
					break;


				}
	//responder 0 si todo bien, 1 si salio mal
*/
	return NULL;
}

int crearConexionLFS(){
	//crea un socket para comunicarse con lfs, devuelve el file descriptor conectado
	int puertoFS = config_get_int_value(configuracion,"PUERTO_FS");
	int ipFS = config_get_int_value(configuracion,"IP_FS");

	u_int16_t *lfsServer;
	createSocket(lfsServer);
	struct sockaddr_in direccionLFS;
	direccionLFS = completServer(ipFS, puertoFS);
	int rta = conectClient(lfsServer, direccionLFS);
	if(rta == 1){
		return -1;
	}
	return lfsServer;
}

void recibirRespuesta(){

}



int main(void) {


	inicializar();
	segmento *animales = crearSegmento("ANIMALES");
	segmento *postres = crearSegmento("POSTRES");
	list_add(tablaSegmentos, animales);
	list_add(tablaSegmentos, postres);

	mInsert("ANIMALES", 1, "GATO");
	mInsert("ANIMALES", 2, "MONO");
	mInsert("POSTRES",5,"FLAN");
	mostrarMemoria();



	u_int16_t puertoServer = config_get_int_value(configuracion, "PUERTO");
	char* ipServer = config_get_string_value(configuracion, "IP");
	u_int16_t server;

	int servidorCreado = createServer(ipServer, puertoServer, &server);


	listen(server,100);
	printf("Servidor escuchando\n");


	struct sockaddr_in kernelCliente;

	unsigned int tamanioDireccion=sizeof(kernelCliente);

	for(int i =0; i <1; i++){

	u_int16_t cliente;
	acceptConexion(server, &cliente,0);

	printf("Se conecto un cliente\n");
	char *buffer = malloc(15);
	char* puto = malloc(14);


	log_info(logger,"sock: %i",cliente);
	int b= recv(cliente, buffer, 14, 0);
	//int b = recvData(cliente, buffer, 14);
	//buffer[b]= "\0";
	//strcpy(puto, buffer);
	log_info(logger, "Bytes recibidos: %d", b);
	log_info(logger, "Buffer %s\n", buffer);


	//pthread_t unHilo;
	//pthread_create(&unHilo, NULL, recibirOperacion, &cliente);
	//pthread_join(unHilo, NULL);

		}



	finalizar();

	return EXIT_SUCCESS;
}

//---------------------------------------------------------//
//------------------AUXILIARES DE ARRANQUE----------------//
//-------------------------------------------------------//

void inicializar(){
	logger = init_logger();
	configuracion = read_config();
	int tamanioMemoria = config_get_int_value(configuracion, "TAM_MEM");
	//int puertoFS = config_get_int_value(configuracion,"PUERTO_FS");
	//int ipFS = config_get_int_value(configuracion,"IP_FS");

	posicionUltimoUso = 0; //Para arrancar la lista de usos en 0, ira aumentando cuando se llene NO TOCAR PLS


	log_info(logger, "Se inicializo la memoria con tamanio %d", tamanioMemoria);
	memoria = calloc(1,tamanioMemoria);
	maxValue = 20;
	//maxValue = handshakeConLissandra(puertoFS,ipFS);

	log_info(logger, "Tamanio máximo recibido de FS: %d", maxValue);

	offsetMarco = sizeof(long) + sizeof(u_int16_t) + maxValue;
	tablaMarcos = list_create();
	tablaSegmentos = list_create();
	listaDeUsos = list_create();



	//Inicializar los marcos
	cantMarcos = tamanioMemoria/offsetMarco;
	for(int i=0; i<cantMarcos; i++){
			marco* unMarco = malloc(sizeof(marco));
			unMarco->nroMarco = i;
			unMarco->estaLibre = 0;
			list_add(tablaMarcos, unMarco);
		}


}

segmento *crearSegmento(char* nombre){
	segmento *nuevoSegmento = malloc(sizeof(segmento));
	nuevoSegmento->nombreTabla = malloc(strlen(nombre)+1);
	strcpy(nuevoSegmento->nombreTabla,nombre);
	nuevoSegmento->tablaPaginas = list_create();
	log_info(logger, "Se creo el segmento %s", nombre);

	return nuevoSegmento;
}

pagina *crearPagina(){
	pagina *pag = malloc(sizeof(pagina));
	pag->nroMarco = primerMarcoLibre();
	if(pag->nroMarco == -1){
		log_error(logger, "No hay espacio para crear una pagina");
		//acá hay que revisar como manejar el error
	}
	return pag;
}

void agregarPagina(segmento *seg, pagina *pag){

	list_add(seg->tablaPaginas, pag);
	log_info(logger, "Se agrego una página al segmento %s", seg->nombreTabla);

}

segmento *buscarSegmento(char* nombre){

	int tieneMismoNombre(segmento *seg){
		int rta = 0;
		if(strcmp((seg->nombreTabla), nombre) ==0){
			rta = 1;
		}

		return rta;
	}
	return list_find(tablaSegmentos, (void *) tieneMismoNombre);
}

pagina *buscarPaginaConKey(segmento *seg, u_int16_t key){

	int tieneMismaKey(pagina *pag){
			int rta = 0;
			u_int16_t offset = (offsetMarco * (pag->nroMarco)) + sizeof(long);
			u_int16_t keyPag = *(u_int16_t*) (memoria + offset);
			if(keyPag == key){
				rta = 1;
			}

			return rta;
		}

	return list_find(seg->tablaPaginas, (void *) tieneMismaKey);

}

t_config* read_config() {
	return config_create("/home/utnso/tp-2019-1c-donSaturados/memoryPool/memoryPool.config");
}

 t_log* init_logger() {
	return log_create("memoryPool.log", "memoryPool", 1, LOG_LEVEL_INFO);
}



 //-----------------------------------------------------------//
 //------------------AUXILIARES DE LFS/KERNEL----------------//
 //---------------------------------------------------------//


 char* empaquetar(int operacion, char* paquete){

	 char* msj;
	 int tamanioPaquete = strlen(paquete)+1;
	 char* tamanioFormateado;
	 if(tamanioPaquete<10){
		 	tamanioFormateado = string_from_format("%i00%i", operacion, tamanioPaquete);
		 }
	 if(tamanioPaquete>=10 && tamanioPaquete<100){
	 	tamanioFormateado = string_from_format("%i0%i", operacion, tamanioPaquete);
	 }
	 if(tamanioPaquete>=100){
		tamanioFormateado = string_from_format("%i%i", operacion, tamanioPaquete);

	 }

	 msj = string_from_format("%s%s", tamanioFormateado, paquete);
 	 return msj;

 }

 char* formatearSelect(char* nombreTabla, u_int16_t key){
	char* paquete = string_from_format("%s;%s", nombreTabla, string_itoa(key));
	return paquete;
}
 char* formatearInsert(char* nombreTabla, long timestamp, u_int16_t key, char* value){
	char* paquete = string_from_format("%s;%s;%s;%s", nombreTabla, string_itoa(timestamp), string_itoa(key), value);
	return paquete;
}

 char* formatearCreate(char* nombreTabla, char* consistencia, int particiones, long tiempoCompactacion){
	char* paquete= string_from_format("%s;%s;%s;%s", nombreTabla, consistencia, string_itoa(particiones), string_itoa(tiempoCompactacion));
	return paquete;
}



 void handshakeConLissandra(u_int16_t lfsCliente,char* ipLissandra,u_int16_t puertoLissandra){
 	int conexionExitosa;
 	int id = 1;
 	conexionExitosa = linkClient(&lfsCliente,ipLissandra , puertoLissandra,id);

 		if(conexionExitosa !=0){
 			perror("Error al conectarse con LFS");
 		}

 		recvData(lfsCliente, &maxValue, sizeof(u_int16_t));
 }


 char* selectLissandra(char* nombreTabla,u_int16_t key){
	 char* datos = formatearSelect(nombreTabla, key);
	 char* paqueteListo = empaquetar(0, datos);
	 u_int16_t lfsSock = crearConexionLFS();
	 if(lfsSock == -1){
		 log_error(logger, "No se pudo conectar con LFS");
	 }
	 send(lfsSock, paqueteListo, strlen(paqueteListo), 0);
	 	 //recibir rta
	 //recibir value
	 //insertar el value
	 //responder a kernel
	 char* valueRecibido;
	 return valueRecibido;
 }

 int insertLissandra(char* nombreTabla, long timestamp, u_int16_t key, char* value){

	 char* datos = formatearInsert(nombreTabla, timestamp, key, value);
	 char* paqueteListo = empaquetar(1, datos);
	 //BORRAR PRINTF
	 printf("EL PAQUETE ES: %s\n", paqueteListo);
	 u_int16_t lfsSock = crearConexionLFS();
	 if(lfsSock == -1){
		 log_error(logger, "No se pudo conectar con LFS");
	 }
	 send(lfsSock, paqueteListo, strlen(paqueteListo), 0);
	 //recibir rta
	 close(lfsSock);

 }

 void createLissandra(char* nombreTabla, char* criterio, u_int16_t nroParticiones, long tiempoCompactacion){
	 char* datos = formatearCreate(nombreTabla, criterio, nroParticiones, tiempoCompactacion);
	 char* paqueteListo = empaquetar(2, datos);
	 u_int16_t lfsSock = crearConexionLFS();
	 if(lfsSock == -1){
		 log_error(logger, "No se pudo conectar con LFS");
	 }
	 send(lfsSock, paqueteListo, strlen(paqueteListo), 0);
	 //recibir rta

	 close(lfsSock);
 }

 void dropLissandra(char* nombreTabla){
	 char* paqueteListo = empaquetar(4, nombreTabla);
	 u_int16_t lfsSock = crearConexionLFS();
	 if(lfsSock == -1){
	 	 log_error(logger, "No se pudo conectar con LFS");
	  }
	 send(lfsSock, paqueteListo, strlen(paqueteListo), 0);
	 //recibir rta

 }


 //---------------------------------------------------------//
 //------------------MANEJO DE MEMORIA---------------------//
 //-------------------------------------------------------//


 int memoriaLlena(){ //Devuelve 0 si esta llena

 	int algunoLibre(marco* unMarco){
 		return unMarco->estaLibre == 0;
 	}

 	return list_any_satisfy(tablaMarcos,(void*)algunoLibre);

 }

 void agregarDato(long timestamp, u_int16_t key, char* value, pagina *pag){

 	int offset = offsetMarco*(pag->nroMarco);
 	memcpy(memoria+offset, &timestamp, sizeof(long));
 	offset = offset + sizeof(long);
 	memcpy(memoria+offset, &key,sizeof(u_int16_t));
 	int offset2 = offset + sizeof(u_int16_t);
 	memcpy(memoria+offset2, value, strlen(value)+1);
 	log_info(logger, "Se agrego el dato %s al marco %d", value, pag->nroMarco);
 }


 int primerMarcoLibre(){
 	int posMarco = -1;
 	int i=0;
 	marco *unMarco;

 	if(memoriaLlena()){ //Si la memoria no esta llena puede asignar un marco

 		while(i < cantMarcos){
 			unMarco = list_get(tablaMarcos,i);
 			if((unMarco->estaLibre) == 0){
 				unMarco->estaLibre = 1; //Ya lo ocupo desde aca.
 				posMarco = unMarco->nroMarco;
 				break;
 			}
 			else{
 				i++;
 			}
 		}
 	}
 	else{
 		//No hace falta llamar el journal aca ya que esta adentro del LRU
 		posMarco = LRU();
 		unMarco->estaLibre = 1;
 	}

 	return posMarco;
 }


 void liberarMarco(int nroMarcoALiberar){
 	marco* nuevo = list_get(tablaMarcos,nroMarcoALiberar);
 	nuevo->estaLibre = 0;
 }


 posMarcoUsado* crearPosMarcoUsado(int nroMarco,int pos){
	 posMarcoUsado* nuevo = malloc(sizeof(posMarcoUsado));
	 nuevo->nroMarco = nroMarco;
	 nuevo->posicionDeUso = pos;
	 return nuevo;
 }

 void agregarPosMarcoUsado(posMarcoUsado* nuevo){
	 list_add(listaDeUsos,nuevo);
 }

 int LRU(){

	 int i=0,menor,tamLista,nroMarcoAborrar;
	 posMarcoUsado* aux= list_get(listaDeUsos,0);
	 tamLista = list_size(listaDeUsos);


	 if(tamLista != 0){ //es decir, si la lista de usos esta vacia
		 menor = aux->posicionDeUso;
		 nroMarcoAborrar= aux->nroMarco;

		 while(i<tamLista){
			 aux = list_get(listaDeUsos,i);
			 if(menor > aux->posicionDeUso){
				 menor = aux->posicionDeUso;
				 nroMarcoAborrar = aux->nroMarco;
			 }else{
				 i++;
			 }
		 }
	 }
	 else{
		mJournal();
		nroMarcoAborrar=0;
	    //hacer journal por memoria llena de flags modificados
	    //Devuelve 0 porque como la memoria queda vacia el 0 pasa a ser el primer marco vacio
	 }

	 liberarMarco(aux->nroMarco);

	 return nroMarcoAborrar;

	 //Esta funcion lee de una lista cual fue el marco que hace mas tiempo que no se usa
	 //lo libera y devuelve su posicion para que sea asignado a otra pagina

 }

 void agregarAListaUsos(int nroMarco){
 	posMarcoUsado* nuevo = crearPosMarcoUsado(nroMarco,posicionUltimoUso);
 	posicionUltimoUso++;
 	list_add(listaDeUsos,nuevo);
 }


 void eliminarDeListaUsos(int nroMarcoAEliminar){

 	int index=0;

 	int tieneMismoNro(posMarcoUsado* p){
 		if((p->nroMarco) != nroMarcoAEliminar) index++;
 		return (p->nroMarco) == nroMarcoAEliminar;
 	}

 	posMarcoUsado* nuevo = list_find(listaDeUsos,(void*)tieneMismoNro);

 	list_remove(listaDeUsos,index);

 }
 bool estaModificada(pagina *pag){
 	bool res = false;
 	if(pag->modificado == 1){
 		res = true;
 	}

 	return res;
 }
 //---------------------------------------------------------//
 //------------AUXILIARES SECUNDARIAS Y BORRADO------------//
 //-------------------------------------------------------//


void mostrarMemoria(){
	int desplazador=0 ,i=0;

		while(i<cantMarcos){

			printf("Timestamp: %ld \n", *(long*)((memoria) + desplazador));
			printf("Key: %d \n", *(u_int16_t*)((memoria)+ sizeof(long) + desplazador));
			printf("Value: %s \n", (char*)((memoria) + sizeof(long) + sizeof(u_int16_t)+desplazador));

			desplazador += offsetMarco;
			i++;

		}
}


void* conseguirValor(pagina* pNueva){

	return ((memoria) + sizeof(long) + sizeof(u_int16_t)+ (pNueva->nroMarco)*offsetMarco);
}

void *conseguirTimestamp(pagina *pag){
	return ((memoria) + offsetMarco*pag->nroMarco);
}

void *conseguirKey(pagina *pag){
	return ((memoria) + sizeof(long) + offsetMarco*pag->nroMarco);
}




int conseguirIndexSeg(segmento* nuevo){

	int index=0;

	while(index < (tablaSegmentos->elements_count)){
		segmento* aux = list_get(tablaSegmentos,index);
		if(string_equals_ignore_case(aux->nombreTabla,nuevo->nombreTabla)){
			break;
		}
		else{
			index++;
		}
	}
	return index;
}

void eliminarSegmento(segmento* nuevo){

	int index = conseguirIndexSeg(nuevo); //se usa para el free

	list_remove_and_destroy_element(tablaSegmentos,index,(void*)segmentoDestroy);


}

void paginaDestroy(pagina* pagParaDestruir){
	if((pagParaDestruir->modificado)==0){
	  	eliminarDeListaUsos(pagParaDestruir->nroMarco);
	 }
	liberarMarco(pagParaDestruir->nroMarco);
	free(pagParaDestruir);
}

void segmentoDestroy(segmento* segParaDestruir){
   list_destroy_and_destroy_elements(segParaDestruir->tablaPaginas,(void*)paginaDestroy);
	free(segParaDestruir->nombreTabla);
	free(segParaDestruir);
}


void eliminarMarcos(){
		list_destroy_and_destroy_elements(tablaMarcos,(void*)marcoDestroy);

}

void marcoDestroy(marco *unMarco){
	free(unMarco);
}

void finalizar(){
	log_info(logger, "Limpiando la memoria");
	for(int i = 0; i<(tablaSegmentos->elements_count); i++){
		segmento *seg = list_get(tablaSegmentos, i);
		eliminarSegmento(seg);
	}
	free(tablaSegmentos);
	eliminarMarcos();

	free(memoria);
	config_destroy(configuracion);
	log_info(logger, "Memoria limpia, adiós mundo cruel");
	log_destroy(logger);

}




//-------------------------------------//
//---------------API------------------//
//-----------------------------------//



void mInsert(char* nombreTabla, u_int16_t key, char* valor){

	segmento *seg = buscarSegmento(nombreTabla);
	pagina *pag;
	long timestampActual;

	if(seg != NULL){

		pag = buscarPaginaConKey(seg, key);
			if (pag == NULL){
				pag = crearPagina();
				agregarPagina(seg,pag);
				timestampActual = time(NULL);
				agregarDato(timestampActual, key, valor, pag);
				pag->modificado = 1;
			}else{
				agregarDato(time(NULL),key,valor,pag);
				pag->modificado = 1;
				eliminarDeListaUsos(pag->nroMarco);
			}

	}else{
		seg = crearSegmento(nombreTabla);
		list_add(tablaSegmentos, seg);
		pagina *pag = crearPagina();
		agregarPagina(seg,pag);
		timestampActual = time(NULL);
		agregarDato(timestampActual, key, valor, pag);
		pag->modificado = 1;
	}
	log_info(logger, "Se inserto al segmento %s el valor %s", nombreTabla, valor);

}

void mSelect(char* nombreTabla,u_int16_t key){
	//BORRAR LOS PRINTFS

	segmento *nuevo = buscarSegmento(nombreTabla);
	pagina* pNueva;
	char* valorPagNueva;
	char* valor;

	if(nuevo!= NULL){

		pNueva = buscarPaginaConKey(nuevo,key);

		if(pNueva != NULL){
			valor = (char*)conseguirValor(pNueva);
			printf("El valor es: %s\n", valor);
			log_info(logger, "Se seleccionó el valor %s", valor);
		}
		else{
			pNueva = crearPagina();
			valorPagNueva = selectLissandra(nombreTabla,key); //Algun dia la haremos y sera hermosa
			pNueva->modificado = 0;
			agregarPagina(nuevo,pNueva);
			agregarDato(time(NULL),key,valorPagNueva,pNueva);
			agregarAListaUsos(pNueva->nroMarco);
			valor = (char*)conseguirValor(pNueva);
			printf("El valor es: %s\n", valor);
			log_info(logger, "Se seleccionó el valor %s", valor);

		}
	}
	else{
		nuevo = crearSegmento(nombreTabla);
		pNueva = crearPagina();
		valorPagNueva = selectLissandra(nombreTabla,key);
		pNueva->modificado = 0;
		agregarPagina(nuevo,pNueva);
		agregarDato(time(NULL),key,valorPagNueva,pNueva);
		agregarAListaUsos(pNueva->nroMarco);
		printf("El valor es: %s\n",valorPagNueva);
		log_info(logger, "Se seleccionó el valor %s", valorPagNueva);

	}


}

void mCreate(char* nombreTabla, char* criterio, u_int16_t nroParticiones, long tiempoCompactacion){

	createLissandra(nombreTabla,criterio,nroParticiones,tiempoCompactacion);

	//El enunciado solo dice que le informe a lissandra, no dice nada de guardar la tabla en memoria
	//Habria que modificar empaquetar para poder mandar criterio,nroParticiones y tiempoCompactacion
}


void mDescribe(char* nombreTabla){
	printf("Hola soy describe");
	//mandar a lfs
	//recibir metadata
	//mandarle a kernel
	//decidir el sabado cómo vamos a armar este pasamanos

}

void mDrop(char* nombreTabla){


	segmento* nuevo = buscarSegmento(nombreTabla);

	if(nuevo != NULL){

		eliminarSegmento(nuevo);
		log_info(logger, "Se realizo un drop del segmento %s", nombreTabla);

	}
	dropLissandra(nombreTabla);

}


void mJournal(){
	log_info(logger, "Inicio del journal, se bloquea la tabla de segmentos");
	//bloquear tabla de segmentos entera
	for(int i =0; i<(tablaSegmentos->elements_count); i++){
		char* nombreSegmento;
		segmento *seg = list_get(tablaSegmentos, i);
		nombreSegmento = seg->nombreTabla;
		t_list *paginasMod;
		paginasMod = list_filter(seg->tablaPaginas, (void*)estaModificada);

		for(int j=0; j<(paginasMod->elements_count); j++){
			pagina *pag = list_get(paginasMod, j);
			long timestamp = *(long*)conseguirTimestamp(pag);
			u_int16_t key = *(u_int16_t*)conseguirKey(pag);
			char* value = (char*)conseguirValor(pag);
			insertLissandra(nombreSegmento, timestamp, key, value);


		}
		list_destroy(paginasMod);
	}
	log_info(logger, "Fin del journal, procede a borrar datos existentes");
	list_clean_and_destroy_elements(tablaSegmentos, (void*)segmentoDestroy);
	log_info(logger, "Datos borrados, se desbloquea la tabla de segmentos");
}

void buscarMemorias(){}

void mGossip(){
	int retardo = config_get_int_value(configuracion, "RETARDO_GOSSIPING");

		while(1){
			buscarMemorias();
			sleep(retardo);
		}
}

