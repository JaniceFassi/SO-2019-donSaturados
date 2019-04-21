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

	t_log* logger = init_logger();	//HAY QUE ACORDARSE DE CERRAR EL LOGGER EN ALGUN MOMENTO
	t_config* config = read_config();

	//memtable = list_create();	//NO SE SI ESTO DEBERIA ESTAR ACA

	char* ip=config_get_string_value(config, "IP");
	log_info(logger, ip);
	char* port=config_get_string_value(config, "PORT");
	log_info(logger, port);
	u_int16_t  server;
	char* value=config_get_string_value(config, "TAMVALUE");
	log_info(logger, value);
	createServer(ip,atoi(port),&server);
	//PRUEBA CON LA LIBRERIA
/*	if(createServer(ip,atoi(port),&server)!=0){
		log_info(logger, "No se pudo crear el server por el puerto o el bind");
	}else{
		log_info(logger, "Se pudo crear el server");
	}

	listenForClients(server, 100);

	u_int16_t socket_client;

	char* serverName=config_get_string_value(config, "NAME");

	if(acceptConexion( server, &socket_client,serverName,1, atoi(value))!=0){
		log_info(logger, "Error en el acept o al enviar handshake");
	}else{
		log_info(logger, "Se acepto la conexion");
	}

	char* buffer = malloc(sizeof(char)*11);//BASADO EN LO Q HICIERON LAS CHICAS

	if(recvData(socket_client, buffer,10)!=0){
		log_info(logger, "Error al recibir la informacion");
	}else{
		log_info(logger, buffer);
	}
	*/
/*	struct sockaddr_in direccionServidor;

	direccionServidor.sin_family=AF_INET;
	direccionServidor.sin_addr.s_addr=inet_addr(ip);
	direccionServidor.sin_port=htons(atoi(port));

	server= socket(AF_INET,SOCK_STREAM,0);
	if(server==-1){
		log_info(logger, "Error al crear el socket\n");
		return 1;
	}
	int activado=1;
	if(setsockopt((int)server, SOL_SOCKET, SO_REUSEADDR, (void*)&activado, (socklen_t)sizeof(activado))==-1){
		log_info(logger, "Error al setear el servidor\n");
		return 1;
	}
	//INICIO DEL SERVIDOR
	//VER SI SE PUEDE HACER EN UNA FUNCION APARTE, PARA NO TENERLO EN EL MAIN


	if(bind((int)server, (void*) &direccionServidor, sizeof(direccionServidor))!=0){
		log_info(logger, "Error al iniciar el servidor");
		perror("");
		return 1;
	}

	listen((int)server,100);

	log_info(logger, "Servidor escuchando");

	struct sockaddr_in direccionCliente;
	unsigned int tamanioDireccion= sizeof(direccionCliente);

	int cliente = accept(server, (void*) &direccionCliente, (socklen_t*)&tamanioDireccion);
	if(cliente<0){
		log_info(logger,"error en accept");
		return 1;
	}
	log_info(logger, "Se conecto un cliente\n");
	///ENVIA HADSHAKE
	int handshake=1;
	char message [50]="Hola el value es: ";
	strcat(message,value);
	if (handshake!=0){
		if(send(cliente,message,strlen(message),0)==-1){ //PASAR POR PARAMETRO EL NOMBRE DEL SERVIDOR
			log_info(logger,"Error al enviar handshake");
			return 1;
		}
	}

	//RECIBE DATOS

	char* buffer = malloc(sizeof(char)*11);

	int bytesRecibidos=0;
	while(1){
		bytesRecibidos = recv(cliente, buffer, 10, 0);
		if(bytesRecibidos<=0){
			perror("Fallo la conexion\n");
			return 1;
		}

		buffer[bytesRecibidos]='\0';
		printf("Recibi %d bytes con %s\n", bytesRecibidos, buffer);

	}

	free(buffer);*/

	return EXIT_SUCCESS;
}

t_log* init_logger() {
	return log_create("lissandra.log", "Lissandra", 1, LOG_LEVEL_INFO);
}

t_config* read_config() {
	return config_create("/home/utnso/tp-2019-1c-donSaturados/LFS/LFS.config");
}

void exec_api(op_code mode){	//Averiguar si se puede hacer asi, segun el header que le llegue llama a la funcion correspondiente (en api o fs??)

	switch(mode){				//Tendria que haber una funcion comun. Ver ejemplo en las commons log que hay funciones template

	case 0:
		break;
	case 1:
		break;
	case 2:
		break;
	case 3:
		break;
	case 4:
		break;
	default:
		break;
	}
}

