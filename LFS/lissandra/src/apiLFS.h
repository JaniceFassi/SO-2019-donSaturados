/*
 * api.h
 *
 *  Created on: 9 abr. 2019
 *      Author: utnso
 */

#ifndef APILFS_H_
#define APILFS_H_
#include <stdio.h>
#include <stdlib.h>

typedef enum{
	SELECT,
	INSERT,
	CREATE,
	DESCRIBE,
	DROP
}op_code;

void api(op_code option);



#endif /* APILFS_H_ */
