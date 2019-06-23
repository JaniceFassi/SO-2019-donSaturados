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

	mInsert("ANIMALES", 1, "GATO");
	mInsert("ANIMALES", 2, "PERRO");
	mInsert("ANIMALES", 3, "CABALLO");
	mInsert("ANIMALES",4,"LOBO MARINO");
	mInsert("POSTRES",5,"TORTA");
	mostrarMemoria();

	printf("\n");
	printf("Ahora probamos SELECT: \n");

	mSelect("ANIMALES",1);
	mSelect("ANIMALES",2);
	mSelect("POSTRES",5);
	//FUNCIONAAAAAAAA


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


//------------------AUXILIARES----------------//


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
	int i=0;
	marco *unMarco;

	while(i < cantMarcos){
		unMarco = list_get(tablaMarcos,i);
		if((unMarco->estaLibre) == 0){
			unMarco->estaLibre = 1; //Ya lo ocupo desde aca.
			posMarco = unMarco->nroMarco;
			return posMarco;
		}
		else{
			i++;
		}
	}

	//Faltaria ver que pasa si la memoria esta llena, problema del franco del futuro jjejej

}

int hayMarcosLibres(){

//Por ahora queda sin utilizar.

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

	//char* nuevaTime = string_new();
	//char* nuevaKey = string_new();

	//string_append(&nuevaKey,string_itoa(key));

	int offset = offsetMarco*(pag->nroMarco);
	memcpy(memoria+offset, &timestamp, sizeof(long));
	offset = offset + sizeof(long);
	memcpy(memoria+offset, &key,sizeof(u_int16_t));
	int offset2 = offset + sizeof(u_int16_t);
	memcpy(memoria+offset2, value, strlen(value)+1);
	pag->modificado = 1;
}

void ocuparMarco(int nro){

	//Sin utilizar por ahora.

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
			u_int16_t offset = (offsetMarco * (pag->nroMarco)) + sizeof(long);
			u_int16_t keyPag = *(u_int16_t*) (memoria + offset);
			if(keyPag == key){
				rta = 1;
			}

			return rta;
		}

	return list_find(seg->tablaPaginas, (void *) tieneMismaKey);

}

char* conseguirValor(pagina* pNueva){

	return (((char*)(memoria + sizeof(long) + sizeof(u_int16_t)))+((pNueva->nroMarco)*offsetMarco));
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
	long timestampActual;

	if(seg != NULL){

		pagina *pag = buscarPaginaConKey(seg, key);
			if (pag == NULL){
				pag = crearPagina();
				agregarPagina(seg,pag);
				timestampActual = time(NULL);
				agregarDato(timestampActual, key, valor, pag);
				pag->modificado = 1;
			}else{
				agregarDato(time(NULL),key,valor,pag);
			}



	}else{
		seg = crearSegmento(nombreTabla);
		pagina *pag = crearPagina();
		//persistir datos a memoria, ya tengo paja
		agregarPagina(seg, pag);

	}

}


void mSelect(char* nombreTabla,u_int16_t key){

	segmento *nuevo = buscarSegmento(nombreTabla);

	if(nuevo!= NULL){
		pagina* pNueva = buscarPaginaConKey(nuevo,key); //esta maldita fucion no esta bien
		if(pNueva != NULL){
			printf("El valor es: %s\n",conseguirValor(pNueva));
		}
		else{
			pNueva = pedirALissandraPagina(nombreTabla,key); //Algun dia la haremos y sera hermosa
			agregarDato(time(NULL),key,conseguirValor(pNueva),pNueva);
		    printf("El valor es: %s\n",conseguirValor(pNueva));
		}
	}
	else{
		pagina* pNueva = pedirALissandraPagina(nombreTabla,key);
		printf("El valor es: %s\n",conseguirValor(pNueva));
	}

	//Los casos en los que requiera pedir datos a lissandra no funcionan todavia ya que pedirALissandra todavia no esta hecha.

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