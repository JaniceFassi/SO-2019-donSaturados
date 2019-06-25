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

	char* buffer = malloc(sizeof(char));
	//recv(cli, buffer, sizeof(char), NULL);
	//1byteop
	//2bytes tamanioop
	//linea con ;

	int sarlompa = atoi(buffer);

	//printf("hola soy un hilo \n");
	switch (sarlompa) {
				case 0:
					printf("select\n");
					break;
				case 1:
					printf("insert\n");
					break;
				case 2:
					printf("create\n");
				break;

				case 3:
					printf("describe\n");
					break;

				case 4:
					printf("drop\n");
					break;
				case 5:
					printf("journal\n");
					break;

				case 6:
					printf("gossip\n");
					break;


				}
	char* rta = "4";
	send(cli, rta, strlen(rta), 0);
	printf("La rta fue %s \n", rta);

	return NULL;
}





//rta kernel 0 todo bien, 1 todo mal, 2 memoria esta full

int main(void) {


	inicializar();



	segmento *animales = crearSegmento("ANIMALES");
	segmento *postres = crearSegmento("POSTRES");
	list_add(tablaSegmentos, animales);
	list_add(tablaSegmentos, postres);

	mInsert("ANIMALES", 1, "GATO");
	mInsert("ANIMALES", 2, "PERRO");
	mInsert("ANIMALES", 3, "CABALLO");
	mInsert("ANIMALES",4,"LOBO MARINO");
	mInsert("POSTRES",5,"TORTA");
	mostrarMemoria();
	printf("Estado memoria: %i",memoriaLlena());
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
	mInsert("COLORES", 12, "ROJO");
	mostrarMemoria();
	printf("Estado memoria: %i",memoriaLlena());



	/*
	int resultado = 15;

	posMarcoUsado* p1 = crearPosMarcoUsado(0,8);
	posMarcoUsado* p2 = crearPosMarcoUsado(1,2);
	posMarcoUsado* p3 = crearPosMarcoUsado(2,3);
	posMarcoUsado* p4 = crearPosMarcoUsado(3,10);

	agregarPosMarcoUsado(p1);
	agregarPosMarcoUsado(p2);
	agregarPosMarcoUsado(p3);
	agregarPosMarcoUsado(p4);

	resultado = LRU();

	printf("El marco mas viejo es: %i",resultado);

	*/


	//finalizar();

	return EXIT_SUCCESS;
}

//---------------------------------------------------------//
//------------------AUXILIARES DE ARRANQUE----------------//
//-------------------------------------------------------//

void inicializar(){
	//t_log *logger = init_logger();
	t_config *configuracion = read_config();
	int tamanioMemoria = config_get_int_value(configuracion, "TAM_MEM");
	//int puertoFS = config_get_int_value(configuracion,"PUERTO_FS");
	//int ipFS = config_get_int_value(configuracion,"IP_FS");

	posicionUltimoUso = 0; //Para arrancar la lista de usos en 0, ira aumentando cuando se llene NO TOCAR PLS

	memoria = calloc(1,tamanioMemoria);
	maxValue = 20;
	//maxValue = handshakeConLissandra(puertoFS,ipFS);
	offsetMarco = sizeof(long) + sizeof(u_int16_t) + maxValue;
	tablaMarcos = list_create();
	tablaSegmentos = list_create();
	listaDeUsos = list_create();

	//handshake lissandra para que nos de el maxvalue


	//Inicializar los marcos
	cantMarcos = tamanioMemoria/offsetMarco;
	for(int i=0; i<cantMarcos; i++){
			marco* unMarco = malloc(sizeof(marco));
			unMarco->nroMarco = i;
			unMarco->estaLibre = 0;
			list_add(tablaMarcos, unMarco);
		}

	config_destroy(configuracion);
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
	if(pag->nroMarco == -1){
		printf("No hay espacio para crear una pagina");
	}
	return pag;
}

void agregarSegmento(segmento* nuevo){
	list_add(tablaSegmentos,nuevo);
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
 	msj = malloc(sizeof(long)+sizeof(u_int16_t)+strlen(value)+1);
 	msj = string_from_format("%s;%s;%s",timestamp, key, value);
 	//sacar tamanio
 	//concatenar operacion y tamanio del mensaje
 	return msj;
 }




 void handshakeConLissandra(u_int16_t lfsCliente,char* ipLissandra,u_int16_t puertoLissandra){
 	int conexionExitosa;
 	int id; //para que no rompa pero ni idea de donde saco esta vaina
 	conexionExitosa = linkClient(&lfsCliente,ipLissandra , puertoLissandra,id);

 		if(conexionExitosa !=0){
 			perror("Error al conectarse con LFS");
 		}

 		recvData(lfsCliente, &maxValue, sizeof(u_int16_t));
 }




 void pedirleCrearTablaAlissandra(char* nombretabla,char*criterio,u_int16_t nroParticiones,long tiempoCompactacion){}

 void pedirleALissandraQueBorre(char* nombreTabla){}

 char* pedirALissandraPagina(char* nombreTabla,u_int16_t key){}


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
 				break;
 			}
 			else{
 				i++;
 			}
 		}
 	}
 	else{
 		//Habria que hacer un if para ver si esta llena pero CON FLAGS en ese caso JOURNAL
 		posMarco = LRU();
 		unMarco->estaLibre = 1;
 	}

 	return posMarco;
 }

 void eliminarSegmento(segmento* nuevo){

 	int index = conseguirIndexSeg(nuevo); //se usa para el free

 	int cantDePaginas = list_size(nuevo->tablaPaginas);

 	for(int i=0;i<cantDePaginas;i++){
 		pagina* pagAEliminar = list_get(nuevo->tablaPaginas,i);
 		liberarMarco(pagAEliminar->nroMarco);
 	}

 	//list_remove_and_destroy_element(tablaSegmentos,index,(void*)segmentoDestroy);


 }

 void liberarMarco(int nroMarcoALiberar){
 	marco* nuevo = list_get(tablaMarcos,nroMarcoALiberar);
 	nuevo->estaLibre = 0;
 }

 void paginaDestroy(pagina* pagParaDestruir){
	liberarMarco(pagParaDestruir->nroMarco);
 	free(pagParaDestruir);
 }

 void segmentoDestroy(segmento* segParaDestruir){
    list_destroy_and_destroy_elements(segParaDestruir->tablaPaginas,(void*)paginaDestroy);
	free(segParaDestruir->nombreTabla);
 	free(segParaDestruir);
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
		 printf("hacete un jorunal \n");
		 //hacer journal por memoria llena de flags modificados
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

 ////AUXILIARES SECUNDARIAS////

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


char* conseguirValor(pagina* pNueva){

	return (((char*)(memoria + sizeof(long) + sizeof(u_int16_t)))+((pNueva->nroMarco)*offsetMarco));
}//Consigue el value de una pagina especifica



int conseguirIndexSeg(segmento* nuevo){ //Funcion util para cuando haces los free del drop

	int index=0;

	while(index < nuevo->tablaPaginas->elements_count +1){
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

void eliminarMarcos(){
		list_destroy_and_destroy_elements(tablaMarcos,(void*)marcoDestroy);

}

void marcoDestroy(marco *unMarco){
	free(unMarco);
}

void finalizar(){
	free(memoria);
	for(int i = 0; i<tablaSegmentos->elements_count; i++){
		segmento *seg = list_get(tablaSegmentos, i);
		eliminarSegmento(seg);

	}

	eliminarMarcos();
	free(tablaSegmentos);
	free(tablaMarcos);


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

}

void mSelect(char* nombreTabla,u_int16_t key){

	segmento *nuevo = buscarSegmento(nombreTabla);
	pagina* pNueva;
	char* valorPagNueva;

	if(nuevo!= NULL){
		pNueva = buscarPaginaConKey(nuevo,key);
		if(pNueva != NULL){
			printf("El valor es: %s\n",conseguirValor(pNueva));
		}
		else{
			pNueva = crearPagina();
			valorPagNueva = pedirALissandraPagina(nombreTabla,key); //Algun dia la haremos y sera hermosa
			pNueva->modificado = 0;
			agregarPagina(nuevo,pNueva);
			agregarDato(time(NULL),key,valorPagNueva,pNueva);
			agregarAListaUsos(pNueva->nroMarco);
		    printf("El valor es: %s\n",conseguirValor(pNueva));
		}
	}
	else{
		nuevo = crearSegmento(nombreTabla);
		pNueva = crearPagina();
		valorPagNueva = pedirALissandraPagina(nombreTabla,key);
		pNueva->modificado = 0;
		agregarPagina(nuevo,pNueva);
		agregarDato(time(NULL),key,valorPagNueva,pNueva);
		agregarAListaUsos(pNueva->nroMarco);
		printf("El valor es: %s\n",valorPagNueva);
	}

	//Los casos en los que requiera pedir datos a lissandra no funcionan ya que pedirALissandra todavia no esta hecha.

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

		eliminarSegmento(nuevo); //hacer
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

