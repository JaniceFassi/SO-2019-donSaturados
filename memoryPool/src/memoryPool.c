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
/*
void* testeandoHilos(void * arg){

	int *sarlompa = (int *) arg;

	printf("hola soy un hilo \n");
	printf("%d\n", *sarlompa);

	int *numero = (int *)malloc(sizeof(int));
	*numero = 15;


	return numero;
}

*/

segmento *crearSegmento(char* nombre){
	segmento *nuevoSegmento = malloc(sizeof(segmento));
	nuevoSegmento->nombreTabla = nombre;
	nuevoSegmento->tablaPaginas = list_create();

	return nuevoSegmento;
}

segmento *buscarSegmento(t_list *lista, char* nombre){

	int tieneMismoNombre(segmento *seg){
		int rta = 0;
		if(strcmp(seg->nombreTabla, nombre) !=1){
			rta = 1;
		}

		return rta;
	}

	return list_find(lista, (void*)tieneMismoNombre);
}



int main(void) {

	//este 1024 debería salir del archivo de configuración
	int tamanioMemoria = 1024;
	void* memoria = malloc(tamanioMemoria);
	tablaMarcos = list_create();

	int cantMarcos = tamanioMemoria/sizeof(datoTabla);

	//inicializo tabla de marcos

	for(int i=0; i<cantMarcos; i++){
		void* posicion = memoria;
		marco* unMarco = malloc(sizeof(marco));
		unMarco->inicio = posicion; //puntero a posición de memoria donde se guarda el struct datoTabla
		unMarco->offset = sizeof(datoTabla);
		unMarco->modificado = 0;
		list_add(tablaMarcos, unMarco);
		posicion = posicion + sizeof(datoTabla);

	}


	tablaSegmentos = list_create();


	segmento *animales = crearSegmento("ANIMALES");
	printf("%s\n", animales->nombreTabla);
	list_add(tablaSegmentos, animales);

	char* nuevo = "hola";
	printf("%s\n", nuevo);

	segmento *encontrado = buscarSegmento(tablaSegmentos, "ANIMALES");
	printf("vamo loko %s\n", encontrado->nombreTabla);




/*  int sarasa = 10;
	int *resultado;

	pthread_t unHilo;
	pthread_create(&unHilo, NULL, testeandoHilos, &sarasa);
	pthread_join(unHilo, (void*)&resultado);

	//printf("El puntero del hilo es %d \n\n\n", *resultado);


	//DESERIALIZAR Y RECIBIR MENSAJES

	struct sockaddr_in direccionServidor;


			direccionServidor.sin_family=AF_INET;
			direccionServidor.sin_addr.s_addr = inet_addr("127.0.0.1");
			direccionServidor.sin_port=htons(8000);

			int servidor= socket(AF_INET,SOCK_STREAM,0);

			if(servidor==-1){
				perror("Error al crear socket\n");
				return 1;
			}

			int activado=1;
			if(setsockopt(servidor, SOL_SOCKET, SO_REUSEADDR, (void*)&activado, (socklen_t)sizeof(activado))==-1){
				perror("Error al setear el socket\n");
				//log_info(logger, "Error al setear el servidor\n");
				return 1;
			}

			//INICIO DEL SERVIDOR
			if(bind(servidor, (void*) &direccionServidor, sizeof(direccionServidor))!=0){
				perror("Fallo el bind\n");
				//log_info(logger, "Error al iniciar el servidor\n");
				return 1;
			}


			listen(servidor,100);
			printf("Servidor escuchando\n");

			struct sockaddr_in direccionCliente;
			unsigned int tamanioDireccion=sizeof(direccionCliente);

			int cliente = accept(servidor, (void*) &direccionCliente, (socklen_t*)&tamanioDireccion);
			if(cliente<0){
				perror("error en accept");
			}

			printf("Se conecto un cliente\n");
			char* buffer = malloc(sizeof(char));

			int bytesRecibidos=0;
//recibo operacion
			bytesRecibidos = recv(cliente, buffer, 1, 0);

			int op = atoi(buffer);
//recibotamanio
			recv(cliente, buffer, 2, 0);

			int tamanio = atoi(buffer);
//recibo mensaje
			char *nuevoBuffer = malloc(tamanio);
			bytesRecibidos = recv(cliente, nuevoBuffer, tamanio, MSG_WAITALL);

			char** recibido = string_split(nuevoBuffer, ";");
			for(int i =0;  i < 4; i++){
				if(recibido[i+1] != NULL){
					printf("%s\n", recibido[i]);

				}
			}





			switch (op) {
			case 0:
				printf("select");
				break;
			case 1:
				printf("insert");
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

			case 6:
				mGossip();
				break;


			}



			if(bytesRecibidos<=0){
				perror("Fallo la conexion\n");
				return 1;
			}

			nuevoBuffer[bytesRecibidos]='\0';
			//printf("\nRecibi %d bytes con %s\n", bytesRecibidos, buffer);
			printf("\nEl mensaje es %s", nuevoBuffer);



			free(buffer);


*/


























	return EXIT_SUCCESS;
}






























void mSelect(char* nombreTabla,u_int16_t key){
	/*tabla tablaEncontrada = buscarTabla(nombreTabla);{//busca tabla, tabla = segmento
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
*/
}
void mInsert(char* nombreTabla,u_int16_t keyTabla,char* valor){
/*
	if(buscarYreemplazar(keyTabla,valor)){

		list_add(segmentoLista,crearSegmento(nombreTabla,keyTabla,valor));
	}
*/
	//Explicacion:
	//se fija si existe la tabla, en ese caso se fija si ya hay alguien con esa key, si hay alguien lo reemplaza y sino agrega la pagina (not done yet)
	//si no existe la tabla, la crea y la agrega
}



void mCreate(){
	printf("Hola soy create");
}
void mDescribe(){
	printf("Hola soy describe");

}
void mDrop(){
	printf("Hola soy drop");

}
void mJournal(){
	printf("Hola soy journal");

}
void mGossip(){
	printf("Hola soy gossip");

}
