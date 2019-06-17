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

//rta kernel 0 todo bien 1 todo mal

int main(void) {



	inicializar();


	segmento *animales = crearSegmento("ANIMALES");
	segmento *postres = crearSegmento("POSTRES");
	list_add(tablaSegmentos, animales);
	list_add(tablaSegmentos, postres);



	//Pruebas :(

	mInsert("ANIMALES", 150, "GATO");
	mInsert("ANIMALES", 155, "GATO2");

	mostrarMemoria();


	int pos = hayMarcosLibres();
	printf("%i n",pos);
	pos = primerMarcoLibre();
	printf("%i /n",pos);


/*  int sarasa = 10;
	int *resultado;

	pthread_t unHilo;
	pthread_create(&unHilo, NULL, testeandoHilos, &sarasa);
	pthread_join(unHilo, (void*)&resultado);

	//printf("El puntero del hilo es %d \n\n\n", *resultado);



			switch (op) {
			case 0:
				printf("select");
				break;
			case 1:
				printf("insert");
				break;
			case 2:
				printf("create");
				//mCreate(char* nombreTabla, char* criterio, u_int16_t nroParticiones, long tiempoCompactacion);
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


//------------------AUXILIARES-------------//


void mostrarMemoria(){
	int desplazador=0 ,i=0;

		while(i<cantMarcos){


			printf("Timestamp: %ld \n", (*(long*)memoria)+desplazador);
			printf("Key: %d \n", (*(u_int16_t*)(memoria+sizeof(long)))+desplazador);
			printf("Value: %s \n", ((char*)(memoria + sizeof(long) + sizeof(u_int16_t)))+desplazador);

			desplazador += offsetMarco;
			i++;

		}
}

/* No me reconoce la sharedLibrary

void handshakeConLissandra(u_int16_t lfsCliente,char* ipLissandra,u_int16_t puertoLissandra){
	int conexionExitosa;
	int id; //para que no rompa pero ni idea de donde saco esta vaina
	conexionExitosa = linkClient(&lfsCliente,ipLissandra , puertoLissandra,id);

		if(conexionExitosa !=0){
			perror("Error al conectarse con LFS");
		}

		recvData(lfsCliente, &maxValue, sizeof(u_int16_t));
}

*/

segmento *crearSegmento(char* nombre){
	segmento *nuevoSegmento = malloc(sizeof(segmento));
	nuevoSegmento->nombreTabla = nombre;
	nuevoSegmento->tablaPaginas = list_create();

	return nuevoSegmento;
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

pagina *crearPagina(){
	pagina *pag = malloc(sizeof(pagina));
	pag->nroMarco = primerMarcoLibre();
	return pag;
}

void agregarPagina(segmento *seg, pagina *pag){

	list_add(seg->tablaPaginas, pag);

}

int primerMarcoLibre(){
	int posMarco = 0;
	if(hayMarcosLibres()!= NULL){
		posMarco = hayMarcosLibres(); //esta funcion te devuelve la posicion del marco libre
	}
	else{
		//la memoria esta llena todos los marcos ocupados
		//hacer luego jeje
	}

	return posMarco;
}

int hayMarcosLibres(){
	int posMarco = 0;

		int primerVacio(marco *unMarco){
			if(unMarco->estaLibre == 0){
				posMarco = unMarco->nroMarco;
				return true; //es medio rancio eso pero necesito que me modifique el posMarco
			}
		}

		list_find(tablaMarcos,(void*)primerVacio);

		return posMarco;
}

char* empaquetar(int operacion, long timestamp, u_int16_t key, char* value){
	char* msj;
	msj = string_new();
	string_append(&msj,string_itoa(operacion));
	string_append(&msj, ";");
	string_append(&msj,string_itoa(strlen(value)));
	string_append(&msj, ";");
	string_append(&msj, string_itoa(timestamp));
	string_append(&msj, ";");
	string_append(&msj, string_itoa(key));
	string_append(&msj, ";");
	string_append(&msj, value);

	return msj;
}

/*
paqueteRecibido desempaquetar(char* paquete){
	paqueteRecibido nuevo;

	paquete = string_new();

	nuevo.key =  ;
	nuevo.value;
}

*/

void inicializar(){
	t_log *logger = init_logger();
	t_config *configuracion = read_config();
	int tamanioMemoria = config_get_int_value(configuracion, "TAM_MEM");
	int puertoFS = config_get_int_value(configuracion,"PUERTO_FS");
	int ipFS = config_get_int_value(configuracion,"IP_FS");

	memoria = calloc(1,tamanioMemoria); //Modifique el tamanio para que sea un multiplo offsetMarco, 5 marcos por ahora.
	maxValue = 20;
	//maxValue = handshakeConLissandra(puertoFS,ipFS);
	offsetMarco = sizeof(long) + sizeof(u_int16_t) + maxValue;
	tablaMarcos = list_create();
	tablaSegmentos = list_create();

	//handshake lissandra para que nos de el maxvalue


	//Inicializar los marcos
	cantMarcos = tamanioMemoria/offsetMarco;
	for(int i=0; i<cantMarcos; i++){
			marco* unMarco = malloc(sizeof(marco));
			unMarco->nroMarco = i;
			unMarco->estaLibre = 0;
			list_add(tablaMarcos, unMarco);
		}
}

void agregarDato(long timestamp, u_int16_t key, char* value, pagina *pag){
	int offset = offsetMarco*(pag->nroMarco);
	memcpy(memoria+offset, &timestamp, sizeof(long));
	offset = offset + sizeof(long);
	memcpy(memoria+offset, &key, sizeof(u_int16_t));
	int offset2 = offset + sizeof(u_int16_t);
	memcpy(memoria+offset2, value, strlen(value)+1);
	pag->modificado = 1;
	ocuparMarco(pag->nroMarco);


}

void ocuparMarco(int nro){

	int tieneMismoNro(marco *unMarco){
		if(unMarco->nroMarco == nro){
			unMarco->estaLibre = 1;
		}
		return unMarco->nroMarco == nro;
	}

	list_find(tablaMarcos,(void*)tieneMismoNro);
}

pagina *pedirALissandraPagina(char* nombreTabla,u_int16_t key){
	//Algun dia...
}

pagina *buscarPaginaConKey(segmento *seg, u_int16_t key){

	int tieneMismaKey(pagina *pag){
			int rta = 0;
			int offset = (offsetMarco * pag->nroMarco) + sizeof(long);
			u_int16_t keyPag = *(u_int16_t*) memoria+ offset;
			if(keyPag == key){
				rta = 1;
			}

			return rta;
		}

	return list_find(seg->tablaPaginas, (void *) tieneMismaKey);

	//-------------------------------------------------------------------------------//
	//Por que se fija en la tablaSegmentos? No deberia hacerlo en seg->tablaPaginas ???
	//Sino re al pedo pasarle por parametro segmento *seg //EDIT: LO CAMBIE
	//-------------------------------------------------------------------------------//
}

t_config* read_config() {
	return config_create("/home/utnso/tp-2019-1c-donSaturados/memoryPool/memoryPool.config");
}

 t_log* init_logger() {
	return log_create("memoryPool.log", "memoryPool", 1, LOG_LEVEL_INFO);
}


//---------------API------------------//

void mInsert(char* nombreTabla, u_int16_t key, char* valor){

	segmento *seg = buscarSegmento(nombreTabla);

	if(seg != NULL){

		pagina *pag = buscarPaginaConKey(seg, key);
			if (pag == NULL){
				pag = crearPagina();
				agregarPagina(seg,pag);

			}
		long timestampActual = time(NULL);
		agregarDato(timestampActual, key, valor, pag);
		pag->modificado = 1;


	}else{

		seg = crearSegmento(nombreTabla);
		pagina *pag = crearPagina();
		//persistir datos a memoria, ya tengo paja
		agregarPagina(seg, pag);

	}


}

//Se puede cambiar a otra cosa que no sea void?
void* mSelect(char* nombreTabla,u_int16_t key){

	segmento *nuevo = buscarSegmento(nombreTabla);
	pagina *pNueva;
	char* relleno = "Soy relleno";

	if(nuevo!= NULL){
		pNueva = buscarPaginaConKey(nuevo,key);
		if(pNueva != NULL){
			return pNueva->nroMarco; //Como saco el value sabiendo el nroMarco?
		}
		else{
			pNueva = pedirALissandraPagina(nombreTabla,key); //Algun dia la haremos y sera hermosa
			agregarDato(time(NULL),key,relleno,pNueva);
			return pNueva->nroMarco; //Devuelta, como saco el value?
		}
	}
	else{
		pNueva = pedirALissandraPagina(nombreTabla,key);
	}


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



/*

void mCreate(char* nombreTabla, char* criterio, u_int16_t nroParticiones, long tiempoCompactacion){
	//char* msj;
	//msj = empaquetar(2, )
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
*/
