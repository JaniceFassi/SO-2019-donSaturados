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

int main(void) {

	theStart();


/*	u_int16_t socket_client;

	//CREACION DE LA CARPETA PRINCIPAL DE TABLAS



	// ****************PARA USAR TIEMPO DEL DUMP*************

	//
	//alarm(timeDump);
    //signal(SIGALRM, funcionSenial);

	char *path=pathFinal("TABLAS",0);	//DEVUELVE EL PATH HASTA LA CARPETA TABLAS

	if(folderExist(path)!=0){		//SI NO EXISTE LA CARPETA TABLAS DEL FILESYSTEM LA CREA
		if(crearCarpeta(path)!=0){
			free(path);
			theEnd();
			return 1;
		}
		log_info(logger,"\nSe ha creado la carpeta principal.");
	}

	free(path);

	// ***************PARA USAR LA FUNCION PURA****************/

	create("PELICULAS", "SC", 5, 10000);
	insert("PELICULAS", 163, "Nemo", 100);				// 3
	insert("PELICULAS", 10, "Toy Story",10);			// 0
	insert("PELICULAS", 10, "Harry Potter",10);			// 0
	selectS("PELICULAS", 10);				// Harry Potter
	selectS("PELICULAS", 163);					// Nemo
	insert("PELICULAS", 13535, "Titanic",20);			// 0
	selectS("PELICULAS", 13535);					// Titanic
	insert("PELICULAS", 922, "Ratatouille",18);			// 2
	insert("PELICULAS", 4829,"Aladdin",10);				// 5
	insert("PELICULAS", 2516, "Godzilla",1300);			// 1
	insert("PELICULAS", 163, "Buscando a dory",1300);	// 1
	selectS("PELICULAS", 4829);					// Aladdin
	insert("PELICULAS", 3671, "Avatar",1000);			// 1
	selectS("PELICULAS", 163);					// Buscando a dory
	selectS("PELICULAS", 3671);					// Avatar

	/*************************************************************/

	/****************PARA USAR LA CONSOLA******************/

	//console();
	//free(valor);

	/****************PARA USAR CONEXIONES******************/

	//connectMemory(&socket_client);

	//int i=0;
	//while(i<5){
	/*char *buffer=malloc(2);
	int recibidos=recvData(socket_client,buffer,1);

	exec_api(atoi(buffer),socket_client);
	free(buffer);*/
	//i++;
	//}

	theEnd();
	return EXIT_SUCCESS;
}

void theStart(){
	pathInicial="/home/utnso/tp-2019-1c-donSaturados/LFS/LFS.config";		//Inicia variable global de path inicial
	raizDirectorio="/home/utnso";
	logger = init_logger();													//Inicia el logger global
	memtable= list_create();												//Inicia la memtable global
	if(archivoValido(pathInicial)!=0){
		estructurarConfig();						//Si existe el config en el path inicial crea la estructura, si no crea el config
	/*}else{
		crearConfig();
	}*/

	levantarDirectorio();				//Crea el directorio ya teniendo el archivo config listo
	//funMetaLFS();
	}
}

t_log* init_logger() {
	return log_create("lissandra.log", "Lissandra", 1, LOG_LEVEL_INFO);
}

t_config* init_config() {
	return config_create(pathInicial);
}


void estructurarConfig(){							//Lee el config y crea una estructura con esos datos
	t_config *config = init_config();				//Crea una estructura datosConfig en configLissandra, variable global
	configLissandra=malloc(sizeof(datosConfig));
	char *aux= config_get_string_value(config,"PUNTO_MONTAJE");
	configLissandra->puntoMontaje=malloc(strlen(aux)+1);
	strcpy(configLissandra->puntoMontaje,aux);

	configLissandra->tiempoDump=config_get_long_value(config,"TIEMPO_DUMP");
	char *aux2=config_get_string_value(config, "IP");
	configLissandra->Ip=malloc(strlen(aux2)+1);
	strcpy(configLissandra->Ip,aux2);
	configLissandra->puerto= config_get_int_value(config, "PORT");
	configLissandra->id= config_get_int_value(config, "ID");
	configLissandra->idEsperado= config_get_int_value(config, "ID_ESPERADO");
	configLissandra->retardo= config_get_int_value(config, "RETARDO");
	configLissandra->tamValue= config_get_int_value(config, "TAMVALUE");
	config_destroy(config);
}

void borrarDatosConfig(){
	free(configLissandra->Ip);
	free(configLissandra->puntoMontaje);
	free(configLissandra);
}

void crearConfig(){
	log_info(logger,"No existe el arch config, asi q se crea con los datos del checkpoint 3\n");
	configLissandra=malloc(sizeof(datosConfig));
	configLissandra->Ip=malloc(15);
	strcpy(configLissandra->Ip,"192.3.0.58");
	configLissandra->puerto=5005;
	char *aux=malloc(60);
	strcpy(aux,"/home/utnso/lissandra-checkpoint/");
	configLissandra->puntoMontaje=malloc(strlen(aux)+1);
	strcpy(configLissandra->puntoMontaje,aux);
	free(aux);
	configLissandra->retardo=100;
	configLissandra->tamValue=255;
	configLissandra->tiempoDump=60000;
	configLissandra->id=1;
	configLissandra->idEsperado=2;

	FILE* configuracion= fopen(pathInicial,"wb"); //NO SE PORQUE NO ME DEJA CREAR UN ARCHIVO
	fclose(configuracion);

	t_config *config = init_config();

	char *ip=string_duplicate(configLissandra->Ip);
	config_set_value(config,"IP",ip);

	char* port = string_itoa(configLissandra->puerto);
	config_set_value(config,"PORT",port);

	char *punto=string_duplicate(configLissandra->puntoMontaje);
	config_set_value(config,"PUNTO_MONTAJE",punto);

	char *retardo=string_itoa(configLissandra->retardo);
	config_set_value(config,"RETARDO",retardo);

	char *tamVal=string_itoa(configLissandra->tamValue);
	config_set_value(config,"TAMVALUE",tamVal);

	char* tiempoDump = string_from_format("%ld",configLissandra->tiempoDump);
	config_set_value(config,"TIEMPO_DUMP",tiempoDump);

	char *id=string_itoa(configLissandra->id);
	config_set_value(config,"ID",id);

	char *idE=string_itoa(configLissandra->idEsperado);
	config_set_value(config,"ID_ESPERADO",idE);

	config_save(config);
	config_destroy(config);
	free(port);
	free(punto);
	free(ip);
	free(retardo);
	free(tamVal);
	free(tiempoDump);
	free(id);
	free(idE);
}


/*void funMetaLFS(){
	char *path=malloc(strlen(configLissandra->puntoMontaje)+1);
	strcpy(path,configLissandra->puntoMontaje);
	char *meta=malloc(9);
	strcpy(meta,"METADATA");
	char *barra=malloc(2);
	strcpy(barra,"/");
	int base= strlen(path)+1+strlen(meta)+1;
	path=realloc(path,base);
	strcat(path,meta);
	base+=strlen(barra)+1;
	path=realloc(path,base);
	strcat(path,barra);
	base+=strlen(meta)+1;
	path=realloc(path,base);
	strcat(path,meta);
	char *ext=malloc(5);
	strcpy(ext,".bin");
	base+=strlen(ext)+1;
	path=realloc(path,base);
	strcat(path,ext);
	metaLFS=malloc(sizeof(metadataLFS));
	if(archivoValido(path)==1){								//Si existe el archivo crea el metadato de config
		t_config *metadata=config_create(path);
		metaLFS->cantBloques=config_get_int_value(metadata, "BLOCKS");
		metaLFS->tamBloques=config_get_int_value(metadata, "BLOCK_SIZE");
		char *aux=config_get_string_value(metadata,"MAGIC_NUMBER");
		metaLFS->magicNumber=malloc(strlen(aux)+1);
		strcpy(metaLFS->magicNumber,aux);
		free(aux);
		config_destroy(metadata);
	}else{
		metaLFS->cantBloques=5192;							//Si no existe el archivo crea todo desde 0 con datos del checkpoint
		metaLFS->tamBloques=64;
		metaLFS->magicNumber=malloc(10);
		strcpy(metaLFS->magicNumber,"LISSANDRA");
		int metadataF= open(path,O_RDWR); //BINARIO
		close(metadataF);
		t_config *metadataC=config_create(path);
		char* tam;
		strcpy(tam,string_itoa(metaLFS->tamBloques));
		config_set_value(metadataC,"BLOCK_SIZE",tam);
		char* cant;
		strcpy(cant,string_itoa(metaLFS->cantBloques));
		config_set_value(metadataC,"BLOCKS",cant);
		config_set_value(metadataC,"MAGIC_NUMBER",metaLFS->magicNumber);
		config_save(metadataC);
		config_destroy(metadataC);
		free(tam);
		free(cant);
	}
	free(path);
	free(meta);
	free(barra);
	free(ext);
}*/

char* recibirDeMemoria(u_int16_t sock){
	char *tam=malloc(3);
	char * buffer;
	recvData(sock,tam,2);

	buffer=malloc(atoi(tam));
	recvData(sock,buffer,((atoi(tam))));

	free(tam);
	log_info(logger,buffer);
	return buffer;
}

void exec_api(op_code mode,u_int16_t sock){

	char *buffer;
	char **subCadena;

	switch(mode){
	case 0:								//orden: tabla, key

		log_info(logger,"\nSELECT");
		buffer=recibirDeMemoria(sock);
		log_info(logger,buffer);
		subCadena=string_split(buffer, ";");
		log_info(logger,subCadena[0]);
		log_info(logger,subCadena[1]);
		/*char *valor=selectS(subCadena[0],atoi(subCadena[1]));
		printf("\n%s",valor);
		log_info(logger,valor);*/
		break;

	case 1:
		log_info(logger,"\nINSERT");	//Este es el insert que viene con el timestamp
		buffer=recibirDeMemoria(sock);	//orden: tabla, key, value, timestamp
		log_info(logger,buffer);
		subCadena=string_split(buffer, ";");
		log_info(logger,subCadena[0]);
		log_info(logger,subCadena[1]);
		log_info(logger,subCadena[2]);
		log_info(logger,subCadena[3]);
		insert(subCadena[0], atoi(subCadena[1]),subCadena[2],atol(subCadena[3]));

		break;

	case 2:
		log_info(logger,"\nCREATE");	//orden: tabla, consistencia, particiones, tiempoCompactacion
		buffer=recibirDeMemoria(sock);
		log_info(logger,buffer);
		subCadena=string_split(buffer, ";");
		log_info(logger,subCadena[0]);
		log_info(logger,subCadena[1]);
		log_info(logger,subCadena[2]);
		log_info(logger,subCadena[3]);
		create(subCadena[0],subCadena[1],atoi(subCadena[2]),atol(subCadena[3]));
		break;

	case 3:
		log_info(logger,"\nDESCRIBE");	//orden: tabla
		buffer=recibirDeMemoria(sock);
		log_info(logger,buffer);
		//completar
		break;

	case 4:
		log_info(logger,"\nDROP");		//orden: tabla
		buffer=recibirDeMemoria(sock);
		log_info(logger,buffer);

		//drop(buffer);
		break;

	default:
		log_info(logger,"\nOTRO");
		break;
	}
	free(buffer);
	//free(subCadena);
}

void connectMemory(u_int16_t *socket_client){	//PRUEBA SOCKETS CON LIBRERIA
	u_int16_t  server;

	if(createServer(configLissandra->Ip,configLissandra->puerto,&server)!=0){
		log_info(logger, "\nNo se pudo crear el server por el puerto o el bind, %n", 1);
	}else{
		log_info(logger, "\nSe pudo crear el server.");
	}

	listenForClients(server,100);


	if(acceptConexion( server, socket_client,configLissandra->idEsperado)!=0){
		log_info(logger, "\nError en el acept.");
	}else{
		log_info(logger, "\nSe acepto la conexion de %i con %i.",configLissandra->id,configLissandra->idEsperado);
		//aca deberia mandar el tamaño maximo del valor
	}
}

void console(){
	char* linea;
	while(1){
		linea = readline(">");

		if(!strncmp(linea,"SELECT ",7))
		{
			char **subStrings= string_n_split(linea,3," ");
			char *valor=selectS(subStrings[1],atoi(subStrings[2]));
			log_info(logger,valor);
			printf("%s\n",valor);
			free(subStrings);
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
	 			printf("%s",split[3]);
	 			if(insert(split[1],key,split[3],timestamp)==0){
	 				printf("Se realizo el insert\n");			//Calculo el timestamp y el value es la cadena completa
	 			}else{
	 				printf("No se pudo realizar el insert\n");
	 			}
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

	 			if(insert(split[1],key,value,timestamp)==0){
	 				printf("Se realizo el insert\n");
	 			}else{
	 				printf("No se pudo realizar el insert\n");
	 			}

	 			free(espacio);
	 			free(value);
	 		}
	 		free(cadena);
 			free(split);
	 	}

	 	if(!strncmp(linea,"CREATE ",7)){
			char **subStrings= string_n_split(linea,5," ");
			if(create(subStrings[1],subStrings[2],atoi(subStrings[3]),atol(subStrings[4]))==0){
				printf("se pudo crear la tabla\n");
			}else{
				printf("No se pudo crear la tablas\n");
			}
			free(subStrings);
		}

		if(!strncmp(linea,"DESCRIBE ",9)){
			char **subStrings= string_n_split(linea,2," ");
			t_list *tablas;
			if(subStrings[1]==NULL){
				tablas=describe(subStrings[1],0);// 0 si no ponen nombre de una Tabla
			}else{
				tablas=describe(subStrings[1],1);//1 si ponen nombre de Tabla
			}
			free(subStrings);
		}

		if(!strncmp(linea,"DROP ",5)){
			char **subStrings= string_n_split(linea,2," ");
			if(subStrings[1]==NULL){
				log_info(logger,"No se ingreso el nombre de la tabla.");
			}else{
				drop(subStrings[1]);
			}
			free(subStrings);
		}

		if(!strncmp(linea,"exit",5)){
			free(linea);
			theEnd();
			break;
		}
		free(linea);
	}
}

void funcionSenial(int sig){
	log_info(logger,"Comienzo de dump");
	dump();
	log_info(logger,"Finalizacion de dump");
	alarm(configLissandra->tiempoDump);

}

void dump(){
	t_list *dump=list_duplicate(memtable);
	list_clean(memtable);
	int cant=list_size(dump);
	while(cant>0){
		Tabla *dumpT=list_get(dump,cant-1);
		char *path=pathFinal(dumpT->nombre,1);
		if(folderExist(path)==0){
			metaTabla *metadata= leerMetadataTabla(dumpT->nombre);
			t_list *regDepurados=regDep(dumpT->registros);
			list_destroy(dumpT->registros);
			escribirReg(dumpT->nombre,regDepurados,metadata->partitions);
			free(metadata->consistency);
			free(metadata->nombre);
			free(metadata);
			list_destroy_and_destroy_elements(regDepurados,(void *)destroyRegistry);
		}
		free(path);
		free(dumpT->nombre);
		free(dumpT);
		dumpT=NULL;
		cant--;
	}
	list_destroy(dump);
}

void theEnd(){
	if(!list_is_empty(memtable)){
		list_destroy_and_destroy_elements(memtable,(void*)liberarTabla);
	}else{
		list_destroy(memtable);
	}
	borrarDatosConfig();
	borrarMetaLFS();
	log_destroy(logger);
}


