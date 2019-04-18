#ifndef MEMORIAPRINCIPAL_H
#define MEMORIAPRINCIPAL_H


#include <stdio.h>
#include <stdlib.h>
#include <arpa/inet.h>
#include <sys/socket.h>

#include<commons/log.h>
#include<commons/string.h>
#include<commons/config.h>
#include<readline/readline.h>

#include<commons/collections/node.h>
#include<commons/collections/list.h>

t_log* init_logger(void);
t_config* read_config(void);

void mSelect();
void mInsert();
void mCreate();
void mDescribe();
void mDrop();
void mJournal();
void mGossip();

#endif
