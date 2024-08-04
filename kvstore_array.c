
#include "kvstore.h"



array_t Array;

// create
int kvstore_array_create(array_t *arr) {

	if (!arr) return -1;

	arr->array_table = kvstore_malloc(KVS_ARRAY_SIZE * sizeof(struct kvs_array_item));
	if (!arr->array_table) {
		return -1;
	}
	memset(arr->array_table, 0, KVS_ARRAY_SIZE * sizeof(struct kvs_array_item));

	arr->array_idx = 0;

	return 0;
}

// destroy
void kvstore_array_destroy(array_t *arr) {

	if (!arr) return ;

	if (!arr->array_table)
		kvstore_free(arr->array_table);

}



int kvs_array_set(array_t *arr, char *key, char *value) {

	if (arr == NULL || key == NULL || value == NULL) return -1;
	if (arr->array_idx == KVS_ARRAY_SIZE) return -1;

	char *kcopy = kvstore_malloc(strlen(key) + 1);
	if (kcopy == NULL) return -1;
	strncpy(kcopy, key, strlen(key)+1);
	
	char *vcopy = kvstore_malloc(strlen(value) + 1);
	if (vcopy == NULL) {
		kvstore_free(kcopy);
		return -1;
	}
	strncpy(vcopy, value, strlen(value)+1);

	int i = 0;
	for (i = 0;i < arr->array_idx;i ++) {
		if (arr->array_table[i].key == NULL) {

			arr->array_table[i].key = kcopy;
			arr->array_table[i].value = vcopy;
			arr->array_idx ++;

			return 0;
		}
	}

	//LOG("array_idx: %d\n", array_idx);
	if (i < KVS_ARRAY_SIZE && i == arr->array_idx) {
		arr->array_table[arr->array_idx].key = kcopy;
		arr->array_table[arr->array_idx].value = vcopy;
		arr->array_idx ++;
	}

	return 0;
}


char * kvs_array_get(array_t *arr, char *key) {

	int i = 0;
	if (arr == NULL) return NULL;

	for (i = 0;i < arr->array_idx;i ++) {
		if (arr->array_table[i].key == NULL) {
			return NULL;
		}
		if (strcmp(arr->array_table[i].key, key) == 0) {
			return arr->array_table[i].value;
		}
	}

	return NULL;
}


// i > 0 : no exist
int kvs_array_delete(array_t *arr, char *key) {

	int i = 0;
	if (arr == NULL || key == NULL) return -1;

	for (i = 0;i < arr->array_idx;i ++) { 

		if (strcmp(arr->array_table[i].key, key) == 0) {
			
			kvstore_free(arr->array_table[i].value);
			arr->array_table[i].value = NULL;

			kvstore_free(arr->array_table[i].key);
			arr->array_table[i].key = NULL;

			arr->array_idx --;

			return 0;
			
		}
	}

	return i; 
}


int kvs_array_modify(array_t *arr, char *key, char *value) {

	int i = 0;
	if (arr == NULL || key == NULL || value == NULL) return -1;

	for (i = 0;i < arr->array_idx;i ++) {

		if (strcmp(arr->array_table[i].key, key) == 0) {

			kvstore_free(arr->array_table[i].value);
			arr->array_table[i].value = NULL;

			char *vcopy = kvstore_malloc(strlen(value) + 1);
			strncpy(vcopy, value, strlen(value)+1);

			arr->array_table[i].value = vcopy;

			return 0;
		}
		
	}

	return i;
}


int kvs_array_count(array_t *arr) {
	if (!arr) return -1;
	
	return arr->array_idx;
}


