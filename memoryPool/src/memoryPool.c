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

	printf("%i",memoriaLlena());

	printf("\n");
	printf("Ahora probamos SELECT: \n");

	mSelect("ANIMALES",1);
	mSelect("ANIMALES",2);
	mSelect("POSTRES",5);

	printf("\n");
	printf("Ahora probamos DROP: \n");

	mDrop("ANIMALES");
	mInsert("POSTRES",10,"HELADO");
	mInsert("POSTRES",22,"CHOCOLATE");
	mostrarMemoria();


	printf("\n");

	printf("%i",memoriaLlena());

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

//---------------------------------------------------------//
//------------------AUXILIARES DE ARRANQUE----------------//
//-------------------------------------------------------//

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

segmento *crearSegmento(char* nombre){
	segmento *nuevoSegmento = malloc(sizeof(segmento));
	nuevoSegmento->nombreTabla = nombre;
	nuevoSegmento->tablaPaginas = list_create();

	return nuevoSegmento;
}

pagina *crearPagina(){
	pagina *pag = malloc(sizeof(pagina));
	pag->nroMarco = primerMarcoLibre();
	return pag;
}

void agregarPagina(segmento *seg, pagina *pag){

	list_add(seg->tablaPaginas, pag);

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

////AUX PARA LISSANDRA O KERNEL////

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


 void pedirleCrearTablaAlissandra(char* nombretabla,char*criterio,u_int16_t nroParticiones,long tiempoCompactacion){}

 void pedirleALissandraQueBorre(char* nombreTabla){}

 pagina *pedirALissandraPagina(char* nombreTabla,u_int16_t key){}


 ////MANEJAR MEMORIA////

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
 	pag->modificado = 1;
 }


 int primerMarcoLibre(){
 	int posMarco = 0;
 	int i=0;
 	marco *unMarco;

 	if(memoriaLlena()){ //Si la memoria no esta llena puede asignar un marco

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
 	}
 	else{
 		//hacete un journal
 	}

 }

 void eliminarPaginas(segmento* nuevo){

 	int index = conseguirIndexSeg(nuevo);
 	int cantDePaginas = list_size(nuevo->tablaPaginas);

 	for(int i=0;i<cantDePaginas;i++){

 		pagina* pagAEliminar = list_get(nuevo->tablaPaginas,i);
 		liberarMarco(pagAEliminar->nroMarco);
 		//hacer los destroy y free
 	}
	//list_destroy_and_destroy_elements(nuevo->tablaPaginas,(void*)paginaDestroy);
 	//list_remove_and_destroy_element(tablaSegmentos,index,(void*)segmentoDestroy);

 	//Estos dos conchudos no funcionan, en especifico seg y pag Destoy

 }

 void liberarMarco(int nroMarcoALiberar){
 	marco* nuevo = list_get(tablaMarcos,nroMarcoALiberar);
 	nuevo->estaLibre = 0;
 }

 void paginaDestroy(pagina* pagParaDestruir){
 	//free(pagParaDestruir->modificado); CREO que no hacen falta
 	//free(pagParaDestruir->nroMarco);
 	free(pagParaDestruir);
 }

 void segmentoDestroy(segmento* segParaDestruir){
 	//list_destroy_and_destroy_elements(segParaDestruir->tablaPaginas,(void*)paginaDestroy);
 	free(segParaDestruir->nombreTabla);
 	free(segParaDestruir);
 }

 ////AUXILIARES SECUNDARIAS////

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


char* conseguirValor(pagina* pNueva){

	return (((char*)(memoria + sizeof(long) + sizeof(u_int16_t)))+((pNueva->nroMarco)*offsetMarco));
}//Consigue el value de una pagina especifica



int conseguirIndexSeg(segmento* nuevo){ //FUncion util para cuando haces los free del drop

	int index=0;

	while(1==1){
		segmento* aux = list_get(tablaSegmentos,index);
		if(string_equals_ignore_case(aux->nombreTabla,nuevo->nombreTabla)){
			return index;
		}
		else{
			index++;
		}
	}
	//Siempre va a terminar en el RETURN ya que si llegamos a esta funcion es porque existe el segmento SI o SI
}


//-------------------------------------//
//---------------API------------------//
//-----------------------------------//

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

void mCreate(char* nombreTabla, char* criterio, u_int16_t nroParticiones, long tiempoCompactacion){

	pedirleCrearTablaAlissandra(nombreTabla,criterio,nroParticiones,tiempoCompactacion);

	//El enunciado solo dice que le informe a lissandra, no dice nada de guardar la tabla en memoria
	//Habria que modificar empaquetar para poder mandar criterio,nroParticiones y tiempoCompactacion
}


void mDescribe(){
	printf("Hola soy describe");

}

void mDrop(char* nombreTabla){

	segmento* nuevo = buscarSegmento(nombreTabla);

	if(nuevo != NULL){

		eliminarPaginas(nuevo); //hacer
		//Eliminar pagina por pagina de la memoria recorriendola
		//hacer free en las listas

	}
	pedirleALissandraQueBorre(nombreTabla);

}
void mJournal(){
	printf("Hola soy journal");

}
void mGossip(){
	printf("Hola soy gossip");

}

