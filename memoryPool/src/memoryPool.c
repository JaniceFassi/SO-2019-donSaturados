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
	log_info(logger,"sock: %i",cli);

	char *buffer = malloc(2);
	int b = recvData(cli, buffer, sizeof(char));

	log_info(logger, "Bytes recibidos: %d", b);
	log_info(logger, "Operacion %d", atoi(buffer));

	int operacion = atoi(buffer);
	char** desempaquetado;

	if(operacion !=5 && operacion !=6){
		char* tamanioPaq = malloc(sizeof(char)*4);

		recvData(cli,tamanioPaq, sizeof(char)*3);
		int tamanio = atoi(tamanioPaq);

		printf("tamanio paquete en nro %d \n", tamanio);

		char* paquete = malloc(tamanio + sizeof(char));
		recvData(cli, paquete, tamanio);
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
	int resp;
	char* rta;


	switch (operacion) {
				case 0: //SELECT
					nombreTabla = desempaquetado[0];
					key = atoi(desempaquetado[1]);
					rta = mSelect(nombreTabla, key);
					printf("rta %s\n", rta);
					//si el select no es basura
					char* msj = malloc(strlen(rta)+4);
					msj = empaquetar(0, rta);
					printf("mensaje %s \n", msj);
					sendData(cli, msj, strlen(msj)+1);
					//sino sólo mandar un 1
					//sendData(cli, "1", sizeof(char));
					break;

				case 1: //INSERT
					nombreTabla = desempaquetado[0];
					key = atoi(desempaquetado[1]);
					value = desempaquetado[2];
					if(strlen(value)+1> maxValue){
						log_error(logger, "Se intentó insertar un valor mayor al permitido");
						sendData(cli, "1", sizeof(char));

					}else{
						log_info(logger, "Parametros válidos, se hace un insert");
						int resp = mInsert(nombreTabla, key, value);
						sendData(cli, string_itoa(resp), sizeof(char));
					}

					break;

				case 2: //CREATE
					nombreTabla = desempaquetado[0];
					consistencia = desempaquetado[1];
					particiones = atoi(desempaquetado[2]);
					tiempoCompactacion = atol(desempaquetado[3]);
					resp = mCreate(nombreTabla, consistencia, particiones, tiempoCompactacion);
					sendData(cli, string_itoa(resp), sizeof(char));
					break;

				case 3: //DESCRIBE
					nombreTabla = desempaquetado[0];
					//mDescribe(nombreTabla);
					printf("describe\n");
					break;

				case 4: //DROP
					nombreTabla = desempaquetado[0];
					resp = mDrop(nombreTabla);
					sendData(cli, string_itoa(resp), sizeof(char));

					break;

				case 5: //JOURNAL
					mJournal();
					break;

				case 6: //GOSSIP
					//mGossip();
					printf("gossip\n");
					break;


				}
	//responder 0 si todo bien, 1 si salio mal
	close(cli);
	return NULL;
}

void* gestionarConexiones (void* arg){
	//CREA EL SERVIDOR Y ESTÁ CONTÍNUAMENTE ESCUCHANDO Y ACEPTANDO PEDIDOS
	u_int16_t puertoServer = config_get_int_value(configuracion, "PUERTO");
	char* ipServer = config_get_string_value(configuracion, "IP");
	u_int16_t server;

	int servidorCreado = createServer(ipServer, puertoServer, &server);
	listen(server,100);
	printf("Servidor escuchando\n");

	while(1){

		u_int16_t cliente;
		acceptConexion(server, &cliente, 0);

		printf("Se conecto un cliente\n");
		pthread_t atiendeCliente;
		pthread_create(&atiendeCliente, NULL, recibirOperacion, &cliente);
		//pthread_join(atiendeCliente, NULL);


	}




	return NULL;
}



int main(void) {

//TODO hay que abortar si no se puede hacer el handshake o el malloc gigante

	inicializar();

	//pthread_t gossipTemporal;
	//pthread_create(&gossipTemporal, NULL, gossipProgramado, NULL);

	pthread_t hiloConsola;
	pthread_create(&hiloConsola, NULL, consola, NULL);

	mInsert("PROFESIONES", 1, "CIRUJANO");
	pthread_t gestorConexiones;
	pthread_create(&gestorConexiones, NULL, gestionarConexiones, NULL);


	//pthread_t journalTemporal;
	//pthread_create(&journalTemporal, NULL, journalProgramado, NULL);


	pthread_join(hiloConsola, NULL);
	//si el exit de consola "apaga" la memoria, pasar un parámetro que vuelva en el join
	//hacer un if y destruir el resto de los hilos ahí, después finalizar

	pthread_join(gestorConexiones, NULL);
	//pthread_join(journalTemporal, NULL);
	//pthread_join(gossipTemporal, NULL);
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
	u_int16_t puertoFS = config_get_int_value(configuracion,"PUERTO_FS");
	char* ipFS = malloc(15);
	strcpy(ipFS, config_get_string_value(configuracion,"IP_FS"));

	posicionUltimoUso = 0; //Para arrancar la lista de usos en 0, ira aumentando cuando se llene NO TOCAR PLS


	log_info(logger, "Se inicializo la memoria con tamanio %d", tamanioMemoria);
	memoria = calloc(1,tamanioMemoria);
	maxValue = 20;
	u_int16_t lfsServidor;
	//maxValue = handshakeConLissandra(lfsServidor, ipFS, puertoFS);

	//log_info(logger, "Tamanio máximo recibido de FS: %d", maxValue);

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
 //---------------------AUXILIARES DE HILOS------------------//
 //---------------------------------------------------------//

 void* consola(void* arg){

	char* linea;
	while(1){
		linea = readline(">");

 		if(!strncmp(linea,"SELECT ",7))
 		{
 			char **subStrings= string_n_split(linea,3," ");
 			u_int16_t k=atoi(subStrings[2]);
 			mSelect(subStrings[1],k);
 			//liberarSubstrings(subStrings);
 		}

 	 	if(!strncmp(linea,"INSERT ",7)){//INSERT "NOMBRE" 5/ "VALUE"
 	 		char **split= string_n_split(linea,4," ");
 	 		int key= atoi(split[2]);
 	 		char **cadena=string_split(split[3]," ");

 	 		mInsert(split[1],key,split[3]);


 	 		//liberarSubstrings(cadena);
 	 		//liberarSubstrings(split);
 	 	}

 	 	if(!strncmp(linea,"CREATE ",7)){
 			char **subStrings= string_n_split(linea,5," ");
 			u_int16_t particiones=atoi(subStrings[3]);
 			long timeCompaction=atol(subStrings[4]);
 			mCreate(subStrings[1],subStrings[2],particiones,timeCompaction);
 			log_info(logger,"Se hizo CREATE de la tabla: %s.",subStrings[1]);
 			//liberarSubstrings(subStrings);
 		}

 		if(!strncmp(linea,"DESCRIBE",8)){
 			char **subStrings= string_n_split(linea,2," ");
 			mDescribe(subStrings[1]);
 			//liberarSubstrings(subStrings);
 		}

 		if(!strncmp(linea,"DROP ",5)){
 			char **subStrings= string_n_split(linea,2," ");
 			if(subStrings[1]==NULL){
 				log_info(logger,"No se ingreso el nombre de la tabla.");
 			}
 			mDrop(subStrings[1]);
 			log_info(logger,"Se envio el drop a LFS y se borro de memoria la tabla %s");

 			//free(subStrings[0]);
 			//free(subStrings[1]);
 			//free(subStrings);
 		}

 		if(!strncmp(linea,"JOURNAL",6)){
 			mJournal();
 		}

 		if(!strncmp(linea,"MOSTRAR",7)){
 			mostrarMemoria();
 		}

 		if(!strncmp(linea,"exit",5)){
 			free(linea);
 			break;
 		}
 		free(linea);
 	}

 	return NULL;
 }

 void* journalProgramado(void *arg){

 	int retardo = config_get_int_value(configuracion,"RETARDO_JOURNAL")/1000;
 	while(1){
 		sleep(retardo);
 		log_info(logger,"Se realiza un journal programado");
 		mJournal();

 		}

 	return NULL;
 }

 void* gossipProgramado(void* arg){
 	int retardo = config_get_int_value(configuracion, "RETARDO_GOSSIPING")/10000;
 	while(1){
 		mGossip();
 		sleep(retardo);
 	}

 	return NULL;
 }

 //-----------------------------------------------------------//
 //------------------AUXILIARES DE LFS/KERNEL----------------//
 //---------------------------------------------------------//


 char* empaquetar(int operacion, char* paquete){

	 char* msj;
	 int tamanioPaquete = strlen(paquete);
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
	char* paquete = string_from_format("%s;%s;%s;%s", nombreTabla, string_itoa(key), value, string_itoa(timestamp));
	return paquete;
}

 char* formatearCreate(char* nombreTabla, char* consistencia, int particiones, long tiempoCompactacion){
	char* paquete= string_from_format("%s;%s;%s;%s", nombreTabla, consistencia, string_itoa(particiones), string_itoa(tiempoCompactacion));
	return paquete;
}



 u_int16_t handshakeConLissandra(u_int16_t lfsCliente,char* ipLissandra,u_int16_t puertoLissandra){
 	int conexionExitosa;
 	int id = 1;
 	conexionExitosa = linkClient(&lfsCliente, ipLissandra , puertoLissandra,id);

 		if(conexionExitosa !=0){
 			perror("Error al conectarse con LFS");
 		}
 		char* buffer = malloc(sizeof(char)*4);
 		//aca iría el enviar codigo del handshake
 		recvData(lfsCliente, buffer, sizeof(char)*3);
 		u_int16_t maxV = atoi(buffer);
 		return maxV;
 }


 int crearConexionLFS(){
 	//crea un socket para comunicarse con lfs, devuelve el file descriptor conectado
 	int puertoFS = config_get_int_value(configuracion,"PUERTO_FS");
 	char* ipFS = config_get_string_value(configuracion,"IP_FS");

 	u_int16_t lfsServer;
 	int rta = linkClient(&lfsServer, ipFS, puertoFS, 1);

 	if(rta == 0){
 		log_info(logger, "se creo una conexión con lfs");

 	}else{
 		log_info(logger, "error al crear una conexión con lfs");

 	}

 	return lfsServer;
 }

 char* selectLissandra(char* nombreTabla,u_int16_t key){
	 char* datos = formatearSelect(nombreTabla, key);
	 char* paqueteListo = empaquetar(0, datos);
	 u_int16_t lfsSock = crearConexionLFS();
	 if(lfsSock == -1){
		 log_error(logger, "No se pudo conectar con LFS");
	 }
	 sendData(lfsSock, paqueteListo, strlen(paqueteListo));
	 int tamRta = offsetMarco + sizeof(char)*4;
	 char* buffer = malloc(tamRta+1);
	 //recibo si salió todo bien
	 //recibo tamanio para malloquear
	 //recibo pagina empaquetada
	 //desempaquetar en timestamp, key, value
	 //insertar
	 recvData(lfsSock, buffer, tamRta);

	 close(lfsSock);

//Si no existe en lissandra tengo que retornar un value invalido tipo error o el char 1 para que se sepa
	 //que responder a kernel
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
	 sendData(lfsSock, paqueteListo, strlen(paqueteListo));
	 char *buffer = malloc(sizeof(char)*2);
	 recvData(lfsSock, buffer, sizeof(char));
	 close(lfsSock);
	 int rta = atoi(buffer);
	 return buffer;

 }

 int createLissandra(char* nombreTabla, char* criterio, u_int16_t nroParticiones, long tiempoCompactacion){
	 char* datos = formatearCreate(nombreTabla, criterio, nroParticiones, tiempoCompactacion);
	 char* paqueteListo = empaquetar(2, datos);
	 u_int16_t lfsSock = crearConexionLFS();
	 if(lfsSock == -1){
		 log_error(logger, "No se pudo conectar con LFS");
	 }
	 sendData(lfsSock, paqueteListo, strlen(paqueteListo));
	 char* buffer = malloc(sizeof(char)*2);
	 recvData(lfsSock, buffer, sizeof(char));

	 close(lfsSock);
	 int rta = atoi(buffer);
	 return rta;
 }

 int dropLissandra(char* nombreTabla){
	 char* paqueteListo = empaquetar(4, nombreTabla);
	 u_int16_t lfsSock = crearConexionLFS();
	 if(lfsSock == -1){
	 	 log_error(logger, "No se pudo conectar con LFS");
	  }
	 sendData(lfsSock, paqueteListo, strlen(paqueteListo));
	 char* buffer = malloc(sizeof(char)*2);
	 recvData(lfsSock, buffer, sizeof(char));

	 close(lfsSock);
	 int rta = atoi(buffer);
	 return rta;

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

	pthread_mutex_lock(&lockMem);
 	int offset = offsetMarco*(pag->nroMarco);
 	memcpy(memoria+offset, &timestamp, sizeof(long));
 	offset = offset + sizeof(long);
 	memcpy(memoria+offset, &key,sizeof(u_int16_t));
 	int offset2 = offset + sizeof(u_int16_t);
 	memcpy(memoria+offset2, value, strlen(value)+1);
 	pthread_mutex_unlock(&lockMem);
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
		unMarco = list_get(tablaMarcos,0);
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

	 liberarMarco(nroMarcoAborrar);

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


 void actualizarListaDeUsos(int nroMarco){

 	int tieneMismoMarco(posMarcoUsado * aux){
 		return aux->nroMarco == nroMarco;
 	}

 	posMarcoUsado* marcoParaActualizar = list_find(listaDeUsos,tieneMismoMarco);

 	marcoParaActualizar->posicionDeUso = posicionUltimoUso;
 	posicionUltimoUso++;
 }

 bool estaModificada(pagina *pag){
 	bool res = false;
 	if(pag->modificado == 1){
 		res = true;
 	}

 	return res;
 }
 //---------------------------------------------------------//
 //-----------------AUXILIARES SECUNDARIAS-----------------//
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

	//Si no entendi mal seria asi lo que quiere hernan:
	//void* timestamp = malloc(sizeof(long));
	//memcpy(timestamp,((memoria) + offsetMarco*pag->nroMarco),sizeof(long));
	//return timestamp;

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



//---------------------------------------------------------//
//--------------------------BORRADO-----------------------//
//-------------------------------------------------------//


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



int mInsert(char* nombreTabla, u_int16_t key, char* valor){
//esto se hace si la memoria no esta full, hay que hacer esa función
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
	return 0;
	//dejo el 0 hasta que Fran haga lo de memoria full
}



char* mSelect(char* nombreTabla,u_int16_t key){
	//BORRAR LOS PRINTFS

	segmento *nuevo = buscarSegmento(nombreTabla);
	pagina* pNueva;
	char* valorPagNueva;
	char* valor;

	if(nuevo!= NULL){

		pNueva = buscarPaginaConKey(nuevo,key);

		if(pNueva != NULL){
			valor = (char*)conseguirValor(pNueva);
			log_info(logger, "Se seleccionó el valor %s", valor);
			return valor;
			if(pNueva->modificado == 0)actualizarListaDeUsos(pNueva->nroMarco);
		}
		else{
			pNueva = crearPagina();
			valorPagNueva = selectLissandra(nombreTabla,key); //Algun dia la haremos y sera hermosa
			pNueva->modificado = 0;
			agregarPagina(nuevo,pNueva);
			agregarDato(time(NULL),key,valorPagNueva,pNueva);
			agregarAListaUsos(pNueva->nroMarco);
			valor = (char*)conseguirValor(pNueva);
			log_info(logger, "Se seleccionó el valor %s", valor);
			return valor;
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
		log_info(logger, "Se seleccionó el valor %s", valorPagNueva);
		return valorPagNueva;

	}


}

int mCreate(char* nombreTabla, char* criterio, u_int16_t nroParticiones, long tiempoCompactacion){

	int rta = createLissandra(nombreTabla,criterio,nroParticiones,tiempoCompactacion);
	log_info(logger, "La respuesta del create fue %d", rta);

	return rta;
}


void mDescribe(char* nombreTabla){
	printf("Hola soy describe");
	//mandar a lfs
	//recibir metadata
	//mandarle a kernel
	//decidir el sabado cómo vamos a armar este pasamanos

}

int mDrop(char* nombreTabla){


	segmento* nuevo = buscarSegmento(nombreTabla);

	if(nuevo != NULL){

		eliminarSegmento(nuevo);
		log_info(logger, "Se realizo un drop del segmento %s", nombreTabla);

	}
	int rta = dropLissandra(nombreTabla);

	return rta;
}


void mJournal(){
	log_info(logger, "Inicio del journal, se bloquea la tabla de segmentos");
	pthread_mutex_lock(&lockTablaSeg);
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
			//insertLissandra(nombreSegmento, timestamp, key, value); /////////////////////////////////////////////
			//acá hay que responder de a 1 al kernel?

		}
		list_destroy(paginasMod);
	}

	log_info(logger, "Fin del journal, procede a borrar datos existentes");
	list_clean_and_destroy_elements(tablaSegmentos, (void*)segmentoDestroy);
	log_info(logger, "Datos borrados, se desbloquea la tabla de segmentos");
	pthread_mutex_unlock(&lockTablaSeg);
}



//NROMEM;PUERTO;IP SUPER SEND CON TABLA ENTERA
void buscarMemorias(){}

void mGossip(){
	int retardo = config_get_int_value(configuracion, "RETARDO_GOSSIPING");

		while(1){
			buscarMemorias();
			sleep(retardo);
		}
}

