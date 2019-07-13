/*
 ============================================================================
 Name        : Lissandra.c
 Author      : jani_sol
 Version     :
 Copyright   : Your copyright notice
 Description : Hello World in C, Ansi-style
 ============================================================================
 */
#include "Lissandra.h"
#define EVENT_SIZE  ( sizeof (struct inotify_event) + 100 )
#define BUF_LEN     ( 1024 * EVENT_SIZE )

int main(void) {

	theStart();

    // ****************PARA USAR TIEMPO DEL DUMP*************

	alarm(configLissandra->tiempoDump/1000);
	signal(SIGALRM, funcionSenial);

	/****************CONEXIONES******************/

	u_int16_t  server;

	if(createServer(configLissandra->Ip,configLissandra->puerto,&server)!=0){
			log_error(logger, "Error al levantar el servidor.");
			return 1;
		}else{
			log_info(logger, "Se levanto el servidor.");
		}

	listenForClients(server,100);

	pthread_create(&hiloMemoria,NULL,connectMemory,&server);

	/****************PARA USAR LA CONSOLA******************/

	console();

	return EXIT_SUCCESS;
}

void theStart(){

	struct dirent *file;
	struct stat myStat;
	pathInicial=malloc(strlen("/home/utnso/tp-2019-1c-donSaturados/LFS/LFS.config")+1);		//Inicia variable global de path inicial (ruta conocida del config)
	strcpy(pathInicial,"/home/utnso/tp-2019-1c-donSaturados/LFS/LFS.config");
	raizDirectorio=malloc(strlen("/home/utnso")+1);
	strcpy(raizDirectorio,"/home/utnso");
	logger = init_logger();									//Inicia el logger global
	memtable= list_create();								//Inicia la memtable global
	if(archivoValido(pathInicial)!=0){
		estructurarConfig();								//Si existe el config en el path inicial crea la estructura, si no lo crea
	}else{
		crearConfig();										//EN CASO DE QUE NO NOS DEN EL CONFIG. DEBERIA SEGUIR ESTANDO ESTO??
	}
	levantarDirectorio();									//Crea todos los niveles del directorio ya teniendo el archivo config listo
	tablaArchGlobal=list_create();
	directorioP=list_create();
	char *dirTablas=nivelTablas();
	DIR *dir=opendir(dirTablas);							//Examina si ya hay tablas en el fs, las agrega al directorio, comienza la compactacion de c/u
	if(dir!=NULL){
		while((file=readdir(dir))!=NULL){
			stat(file->d_name, &myStat);
			if ( (strcmp(file->d_name, ".")!=0) && (strcmp(file->d_name, "..")!=0) ){
				log_info(logger,"%s es una tabla.",file->d_name);
				metaTabla *metadata=leerMetadataTabla(file->d_name);
				Sdirectorio *uno=malloc(sizeof(Sdirectorio));
				uno->nombre=malloc(strlen(file->d_name)+1);
				uno->time_compact=metadata->compaction_time;
				strcpy(uno->nombre,file->d_name);
				semaforosTabla(uno);
				//sem_wait(criticaDirectorio);
				list_add(directorioP,uno);
				pthread_create(&uno->hilo, NULL, &compactar,uno);
				//sem_post(criticaDirectorio);
				borrarMetadataTabla(metadata);
			}
		}
		closedir(dir);
	}
	free(dirTablas);
	inicializarSemGlob();
}

t_log* init_logger() {
	return log_create("lissandra.log", "Lissandra", 1, LOG_LEVEL_INFO);
}

t_config* init_config() {
	return config_create(pathInicial);
}

void funcionSenial(int sig){
	dump();
	alarm(configLissandra->tiempoDump/1000);
    signal(SIGALRM, funcionSenial);
	return;
}

void *connectMemory(u_int16_t *server){

	u_int16_t socket_client;

	while(1){
		if(acceptConexion( *server, &socket_client,configLissandra->idEsperado)!=0){
			log_info(logger,"Conexion denegada.");
			return NULL;
		}
		log_info(logger, "\nSe acepto la conexion de %i con %i.",configLissandra->id,configLissandra->idEsperado);
		pthread_t unHilo;
		pthread_create(&unHilo,NULL,interactuarConMemoria,&socket_client);
		//pthread_join(unHilo,NULL);
	}

	return NULL;
}

void *interactuarConMemoria(u_int16_t *socket_cliente){
	int header=0;
	char *buffer=malloc(2);
	recvData(*socket_cliente,buffer,1);
	header=atoi(buffer);
	exec_api(header,*socket_cliente);
	free(buffer);
	pthread_exit(NULL);
	return NULL;
}

char* recibirDeMemoria(u_int16_t sock){
	char *tam=malloc(4);
	char * buffer;
	recvData(sock,tam,3);
	int tamanio=atoi(tam);

	if(tamanio==0){
		free(tam);
		return NULL;
	}
	buffer=malloc(tamanio);
	recvData(sock,buffer,((atoi(tam))));

	free(tam);
	return buffer;
}

void *inicializarInotify(){
	char buffer[BUF_LEN];
	int file_descriptor = inotify_init();
	if (file_descriptor < 0) {
		perror("inotify_init");
	}
	int watch_descriptor = inotify_add_watch(file_descriptor, pathInicial, IN_MODIFY );
	int length = read(file_descriptor, buffer, BUF_LEN);
	if (length < 0) {
		perror("read");
	}

	int offset = 0;

	while (offset < length) {
		struct inotify_event *event = (struct inotify_event *) &buffer[offset];
		if (event->len) {
			if (event->mask & IN_MODIFY) {
				if (event->mask & IN_ISDIR) {
					printf("The directory %s was modified.\n", event->name);
				} else {
					printf("The file %s was modified.\n", event->name);
				}
			}
		offset += sizeof (struct inotify_event) + event->len;
		}
	}
	return NULL;
}

void mostrarDescribe(t_list *lista){
	void mostrarD(metaTabla *describe){
		char *nombre=string_from_format("**Nombre de la TABLA: %s",describe->nombre);
		printf("%s \n",nombre);
		char *consistencia=string_from_format("CONSISTENCY= %s",describe->consistency);
		printf("%s \n",consistencia);
		char *particiones=string_from_format("PARTITIONS= %i",describe->partitions);
		printf("%s \n",particiones);
		char *compactacionT=string_from_format("TIME_COMPACTION= %ld",describe->compaction_time);
		printf("%s \n",compactacionT);
		free(nombre);
		free(consistencia);
		free(particiones);
		free(compactacionT);
	}
	list_iterate(lista,(void *)mostrarD);
}

char *empaquetarDescribe(t_list *lista){
	char *paquete=NULL;
	void concatDescribe(metaTabla *describe){
		char *linea=string_from_format("%s;%s;%i;%ld",describe->nombre,describe->consistency,describe->partitions,describe->compaction_time);
		char *aux;
			int tam=strlen(linea)+1;
			if(tam<10){
				aux=string_from_format("00%i%s",tam,linea);
			}else{
				if(tam<100){
					aux=string_from_format("0%i%s",tam,linea);
				}else{
					aux=string_from_format("%i%s",tam,linea);
				}
			}
		if(paquete==NULL){
			paquete=string_from_format("%s",aux);
		}else{
			char *viejo=malloc(strlen(paquete)+1);
			strcpy(viejo,paquete);
			free(paquete);
			paquete=string_from_format("%s;%s",viejo,aux);
			free(viejo);
		}
		free(aux);
		free(linea);
	}
	list_iterate(lista,(void*)concatDescribe);
	return paquete;
}

void exec_api(op_code mode,u_int16_t sock){

	char *buffer;
	char **subCadena;

	switch(mode){
	char *respuesta;
	case 0:								//orden: tabla, key

		log_info(logger,"\nSELECT");
		buffer=recibirDeMemoria(sock);
		subCadena=string_split(buffer, ";");

		int keyBuscada=atoi(subCadena[1]);
		char *valor=lSelect(subCadena[0],keyBuscada);

		if(valor==NULL){
			respuesta=string_from_format("1");
		}else
		{
			if(strlen(valor)<10){
				respuesta=string_from_format("000%i%s",strlen(valor),valor);
			}else{
				if(configLissandra->tamValue<100){
					respuesta=string_from_format("00%i%s",strlen(valor),valor);
				}else{
					respuesta=string_from_format("0%i%s",strlen(valor),valor);
				}
			}
		}
		sendData(sock,respuesta,strlen(respuesta));
		free(respuesta);
		free(valor);
		close(sock);
		break;

	case 1:
		log_info(logger,"\nINSERT");	//Este es el insert que viene con el timestamp
		buffer=recibirDeMemoria(sock);	//orden: tabla, key, value, timestamp
		subCadena=string_split(buffer, ";");
		int key=atoi(subCadena[1]);
		long time=atol(subCadena[3]);
		if(insert(subCadena[0], key,subCadena[2],time)==1){
			respuesta=string_from_format("1");
		}else
		{
			respuesta=string_from_format("0");
		}
		sendData(sock,respuesta,strlen(respuesta));
		free(respuesta);
		close(sock);
		break;

	case 2:
		log_info(logger,"\nCREATE");	//orden: tabla, consistencia, particiones, tiempoCompactacion
		buffer=recibirDeMemoria(sock);
		subCadena=string_split(buffer, ";");
		int part=atoi(subCadena[2]);
		int timeCompact=atol(subCadena[3]);
		if(create(subCadena[0],subCadena[1],part,timeCompact)==1){
			respuesta=string_from_format("1");
		}else
		{
			respuesta=string_from_format("0");
		}
		sendData(sock,respuesta,strlen(respuesta));
		free(respuesta);
		close(sock);
		break;

	case 3:
		log_info(logger,"\nDESCRIBE");	//orden: tabla
		buffer=recibirDeMemoria(sock);
		t_list *tabla;
		if(buffer==NULL){
			tabla=describe(NULL);
		}else{
			tabla=describe(buffer);
		}
		if(list_is_empty(tabla)){
			respuesta=string_from_format("1");
		}else{
			char *paquete=empaquetarDescribe(tabla);
			int cantT=list_size(tabla);
			char *canTablas;
			if(cantT<10){
				canTablas=string_from_format("0%i",cantT);
			}else{
				canTablas=string_itoa(cantT);
			}
			int tamTotal=strlen(paquete)+1+strlen(canTablas);
			if(tamTotal<10){
				respuesta=string_from_format("0000%i%s%s",tamTotal,canTablas,paquete);
			}else{
				if(tamTotal<100){
					respuesta=string_from_format("000%i%s%s",tamTotal,canTablas,paquete);
				}else{
					if(tamTotal<1000){
						respuesta=string_from_format("00%i%s%s",tamTotal,canTablas,paquete);
					}else{
						respuesta=string_from_format("0%i%s%s",tamTotal,canTablas,paquete);
					}
				}
			}
			free(paquete);
			free(canTablas);
		}
		sendData(sock,respuesta,strlen(respuesta));
		free(respuesta);
		close(sock);
		break;

	case 4:
		log_info(logger,"\nDROP");		//orden: tabla
		buffer=recibirDeMemoria(sock);
		if(drop(buffer)==1){
			respuesta=string_from_format("1");
		}else
		{
			respuesta=string_from_format("0");
		}
		sendData(sock,respuesta,strlen(respuesta));
		free(respuesta);
		close(sock);
		break;

	case 5:
		log_info(logger,"\nEXIT");
		close(sock);
		theEnd();
		break;

	case 8:
		log_info(logger,"PRIMER HANDSHAKE CON MEMORIA");
		char *maxValue;
		if(configLissandra->tamValue<10){
			maxValue=string_from_format("00%i",configLissandra->tamValue);
		}else{
			if(configLissandra->tamValue<100){
				maxValue=string_from_format("0%i",configLissandra->tamValue);
			}
			else{
			maxValue=string_from_format("%i",configLissandra->tamValue);
			}
		}

		sendData(sock,maxValue,3);
		free(maxValue);
		close(sock);
		break;

	default:
		log_info(logger,"\nAPI INVALIDA");
		break;

	}
	//free(buffer);							//SE LO SAQUE MOMENTANEAMENTE PARA PROBAR EL EXIT
	//liberarSubstrings(subCadena);			//NO SIEMPRE HAY QUE LIBERAR SUBSTRINGS
}

void *console(){
	char* linea;
	while(1){
		linea = readline(">");

		if(!strncmp(linea,"SELECT ",7))
		{
			char **subStrings= string_n_split(linea,3," ");
			u_int16_t k=atoi(subStrings[2]);
			char *valor=lSelect(subStrings[1],k);
			liberarSubstrings(subStrings);
			free(valor);
		}

	 	if(!strncmp(linea,"INSERT ",7)){//INSERT "NOMBRE" 5/ "VALUE" 156876
	 		char **split= string_n_split(linea,4," ");
	 		int cantPalabras=0;
	 		int key= atoi(split[2]);
	 		char **cadena=string_split(split[3]," ");

	 		while(cadena[cantPalabras]!=NULL){			//Cuento la cantidad de palabras sin tener en cuenta la primera parte
	 			cantPalabras++;							// INSERT nombre key no se toma en cuenta
	 		}

	 		long timestamp=atol(cadena[cantPalabras-1]);

	 		if(timestamp==0){									//NO TIENE TIMESTAMP
	 			timestamp= time(NULL);
	 			insert(split[1],key,split[3],timestamp);	//Calculo el timestamp y el value es la cadena completa
	 		}else{
	 			int base= string_length(cadena[0])+1;
	 			char *value=malloc(base);
	 			char *espacio=malloc(2);
	 			strcpy(espacio," ");
	 			strcpy(value,cadena[0]);

	 			for (int i=1;i<cantPalabras-1;i++){
	 				base +=strlen(espacio)+1;
	 				value=realloc(value,base);
	 				strcat(value,espacio);
	 				base += strlen(cadena[i])+1;
	 				value=realloc(value,base);
	 				strcat(value,cadena[i]);
	 			}

	 			insert(split[1],key,value,timestamp);
	 			free(espacio);
	 			free(value);
	 		}
	 		liberarSubstrings(cadena);
	 		liberarSubstrings(split);
	 	}

	 	if(!strncmp(linea,"CREATE ",7)){
			char **subStrings= string_n_split(linea,5," ");
			u_int16_t particiones=atoi(subStrings[3]);
			long timeCompaction=atol(subStrings[4]);
			if(create(subStrings[1],subStrings[2],particiones,timeCompaction)!=0){
				log_info(logger,"No se pudo crear la tabla %s.",subStrings[1]);
			}
			liberarSubstrings(subStrings);
		}

		if(!strncmp(linea,"DESCRIBE",8)){
			char **subStrings= string_n_split(linea,2," ");
			t_list *tablas;
			tablas=describe(subStrings[1]);
			if(list_is_empty(tablas)){
				printf("No se encontraron descripciones de la/s tablas\n");
				list_destroy(tablas);
			}else{
				mostrarDescribe(tablas);
				list_destroy_and_destroy_elements(tablas,(void *)borrarMetadataTabla);
			}
			liberarSubstrings(subStrings);
		}

		if(!strncmp(linea,"DROP ",5)){
			char **subStrings= string_n_split(linea,2," ");
			if(subStrings[1]==NULL){
				log_info(logger,"No se ingreso el nombre de la tabla.");
			}else{
				if(drop(subStrings[1])!=0){
					log_info(logger,"No se pudo eliminar correctamente la tabla.");
				}
			}
			free(subStrings[0]);
			free(subStrings[1]);
			free(subStrings);
		}

		if(!strncmp(linea,"exit",5)){
			free(linea);
			theEnd();
			break;
		}
		free(linea);
	}
	return NULL;
}

void theEnd(){
	if(!list_is_empty(memtable)){
		list_destroy_and_destroy_elements(memtable,(void*)liberarTabla);
	}else{
		list_destroy(memtable);
	}
	pthread_kill(hiloMemoria,0);
	liberarDirectorioP();
	borrarDatosConfig();
	borrarMetaLFS();
	free(pathInicial);
	free(raizDirectorio);
	close(archivoBitmap);
	liberarSemaforos();
	log_destroy(logger);

}
