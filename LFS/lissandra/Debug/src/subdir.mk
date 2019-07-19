################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../src/Compactor.c \
../src/FileSystem.c \
../src/Lissandra.c \
../src/TADs.c \
../src/apiLFS.c 

OBJS += \
./src/Compactor.o \
./src/FileSystem.o \
./src/Lissandra.o \
./src/TADs.o \
./src/apiLFS.o 

C_DEPS += \
./src/Compactor.d \
./src/FileSystem.d \
./src/Lissandra.d \
./src/TADs.d \
./src/apiLFS.d 


# Each subdirectory must supply rules for building sources it contributes
src/%.o: ../src/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -I/home/utnso/tp-2019-1c-donSaturados/sharedLibrary -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


