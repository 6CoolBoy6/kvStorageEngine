


#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "kvstore.h"

#define RED				1
#define BLACK 			2


#define ENABLE_KEY_CHAR		1

#define MAX_KEY_LEN			256
#define MAX_VALUE_LEN		1024

#if ENABLE_KEY_CHAR
typedef char* KEY_TYPE;
#else
typedef int KEY_TYPE;
#endif


typedef struct _rbtree_node {
	unsigned char color;
	struct _rbtree_node *right;
	struct _rbtree_node *left;
	struct _rbtree_node *parent;

	KEY_TYPE key;
	void *value;
} rbtree_node;

typedef struct _rbtree {
	rbtree_node *root;
	rbtree_node *nil;
	
	int count;
} rbtree;



rbtree_node *rbtree_mini(rbtree *T, rbtree_node *x) {
	while (x->left != T->nil) {
		x = x->left;
	}
	return x;
}

rbtree_node *rbtree_maxi(rbtree *T, rbtree_node *x) {
	while (x->right != T->nil) {
		x = x->right;
	}
	return x;
}

rbtree_node *rbtree_successor(rbtree *T, rbtree_node *x) {
	rbtree_node *y = x->parent;

	if (x->right != T->nil) {
		return rbtree_mini(T, x->right);
	}

	while ((y != T->nil) && (x == y->right)) {
		x = y;
		y = y->parent;
	}
	return y;
}


void rbtree_left_rotate(rbtree *T, rbtree_node *x) {

	rbtree_node *y = x->right;  // x  --> y  ,  y --> x,   right --> left,  left --> right

	x->right = y->left; //1 1
	if (y->left != T->nil) { //1 2
		y->left->parent = x;
	}

	y->parent = x->parent; //1 3
	if (x->parent == T->nil) { //1 4
		T->root = y;
	} else if (x == x->parent->left) {
		x->parent->left = y;
	} else {
		x->parent->right = y;
	}

	y->left = x; //1 5
	x->parent = y; //1 6
}


void rbtree_right_rotate(rbtree *T, rbtree_node *y) {

	rbtree_node *x = y->left;

	y->left = x->right;
	if (x->right != T->nil) {
		x->right->parent = y;
	}

	x->parent = y->parent;
	if (y->parent == T->nil) {
		T->root = x;
	} else if (y == y->parent->right) {
		y->parent->right = x;
	} else {
		y->parent->left = x;
	}

	x->right = y;
	y->parent = x;
}

void rbtree_insert_fixup(rbtree *T, rbtree_node *z) {

	while (z->parent->color == RED) { //z ---> RED
		if (z->parent == z->parent->parent->left) {
			rbtree_node *y = z->parent->parent->right;
			if (y->color == RED) {
				z->parent->color = BLACK;
				y->color = BLACK;
				z->parent->parent->color = RED;

				z = z->parent->parent; //z --> RED
			} else {

				if (z == z->parent->right) {
					z = z->parent;
					rbtree_left_rotate(T, z);
				}

				z->parent->color = BLACK;
				z->parent->parent->color = RED;
				rbtree_right_rotate(T, z->parent->parent);
			}
		}else {
			rbtree_node *y = z->parent->parent->left;
			if (y->color == RED) {
				z->parent->color = BLACK;
				y->color = BLACK;
				z->parent->parent->color = RED;

				z = z->parent->parent; //z --> RED
			} else {
				if (z == z->parent->left) {
					z = z->parent;
					rbtree_right_rotate(T, z);
				}

				z->parent->color = BLACK;
				z->parent->parent->color = RED;
				rbtree_left_rotate(T, z->parent->parent);
			}
		}
		
	}

	T->root->color = BLACK;
}

// int --> char *
void rbtree_insert(rbtree *T, rbtree_node *z) {

	rbtree_node *y = T->nil;
	rbtree_node *x = T->root;

// strcmp() == 0,  < 0, 

	while (x != T->nil) {
		y = x;
#if ENABLE_KEY_CHAR
		if (strcmp(z->key, x->key) < 0) {
			x = x->left;
		} else if (strcmp(z->key, x->key) > 0) {
			x = x->right;
		} else {
			return ;
		}

#else
		if (z->key < x->key) { //strcmp
			x = x->left;
		} else if (z->key > x->key) {
			x = x->right;
		} else { //Exist
			return ;
		}
#endif
	}

	z->parent = y;
	if (y == T->nil) {
		T->root = z;
#if ENABLE_KEY_CHAR
	} else if (strcmp(z->key, y->key) < 0) {
#else
	} else if (z->key < y->key) {
#endif
		y->left = z;
	} else {
		y->right = z;
	}

	z->left = T->nil;
	z->right = T->nil;
	z->color = RED;

	rbtree_insert_fixup(T, z);
}

void rbtree_delete_fixup(rbtree *T, rbtree_node *x) {

	while ((x != T->root) && (x->color == BLACK)) {
		if (x == x->parent->left) {

			rbtree_node *w= x->parent->right;
			if (w->color == RED) {
				w->color = BLACK;
				x->parent->color = RED;

				rbtree_left_rotate(T, x->parent);
				w = x->parent->right;
			}

			if ((w->left->color == BLACK) && (w->right->color == BLACK)) {
				w->color = RED;
				x = x->parent;
			} else {

				if (w->right->color == BLACK) {
					w->left->color = BLACK;
					w->color = RED;
					rbtree_right_rotate(T, w);
					w = x->parent->right;
				}

				w->color = x->parent->color;
				x->parent->color = BLACK;
				w->right->color = BLACK;
				rbtree_left_rotate(T, x->parent);

				x = T->root;
			}

		} else {

			rbtree_node *w = x->parent->left;
			if (w->color == RED) {
				w->color = BLACK;
				x->parent->color = RED;
				rbtree_right_rotate(T, x->parent);
				w = x->parent->left;
			}

			if ((w->left->color == BLACK) && (w->right->color == BLACK)) {
				w->color = RED;
				x = x->parent;
			} else {

				if (w->left->color == BLACK) {
					w->right->color = BLACK;
					w->color = RED;
					rbtree_left_rotate(T, w);
					w = x->parent->left;
				}

				w->color = x->parent->color;
				x->parent->color = BLACK;
				w->left->color = BLACK;
				rbtree_right_rotate(T, x->parent);

				x = T->root;
			}

		}
	}

	x->color = BLACK;
}


// int -->  char *
rbtree_node *rbtree_delete(rbtree *T, rbtree_node *z) {

	rbtree_node *y = T->nil;
	rbtree_node *x = T->nil;

	if ((z->left == T->nil) || (z->right == T->nil)) {
		y = z;
	} else {
		y = rbtree_successor(T, z);
	}

	if (y->left != T->nil) {
		x = y->left;
	} else if (y->right != T->nil) {
		x = y->right;
	}

	x->parent = y->parent;
	if (y->parent == T->nil) {
		T->root = x;
	} else if (y == y->parent->left) {
		y->parent->left = x;
	} else {
		y->parent->right = x;
	}

	if (y != z) {
		
#if ENABLE_KEY_CHAR
		void *tmp = z->key;
		z->key = y->key;
		y->key = tmp;

		tmp = z->value;
		z->value = y->value;
		y->value = tmp;
#else
		z->key = y->key;
		z->value = y->value;
#endif
	}

	if (y->color == BLACK) {
		rbtree_delete_fixup(T, x);
	}

	return y;
}


// int --> char *
rbtree_node *rbtree_search(rbtree *T, KEY_TYPE key) {

	rbtree_node *node = T->root;
	while (node != T->nil) {
		
#if ENABLE_KEY_CHAR
		if (strcmp(key, node->key) < 0) {
			node = node->left;
		} else if (strcmp(key, node->key) > 0) {
			node = node->right;
		} else {
			return node;
		}
#else
		if (key < node->key) {
			node = node->left;
		} else if (key > node->key) {
			node = node->right;
		} else {
			return node;
		}	
#endif
	}
	return T->nil;
}


void rbtree_traversal(rbtree *T, rbtree_node *node) {
	if (node != T->nil) {
		rbtree_traversal(T, node->left);
		printf("key:%s, color:%d\n", node->key, node->color);
		rbtree_traversal(T, node->right);
	}
}





// rbtree api
int kvstore_rbtree_create(rbtree *tree) {

	if (!tree) return -1;
	memset(tree, 0, sizeof(rbtree));
	
	tree->nil = (rbtree_node*)malloc(sizeof(rbtree_node));
	tree->nil->key = malloc(1);
	*(tree->nil->key) = '\0';
	
	
	tree->nil->color = BLACK;
	tree->root = tree->nil;

	return 0;
}

void kvstore_rbtree_destory(rbtree *tree) {

	if (!tree) return ;

	if (tree->nil->key) kvstore_free(tree->nil->key);

	rbtree_node *node = tree->root;
	while (node != tree->nil) {

		node = rbtree_mini(tree, tree->root);
		if (node == tree->nil) {
			break;
		}

		node = rbtree_delete(tree, node);

		if (!node) {
			kvstore_free(node->key);
			kvstore_free(node->value);
			kvstore_free(node);
		}
		

	}

}


int kvs_rbtree_set(rbtree *tree, char *key, char *value) {

	rbtree_node *node  = (rbtree_node*)malloc(sizeof(rbtree_node));
	if (!node) return -1;

	node->key = kvstore_malloc(strlen(key) + 1);
	if (node->key == NULL) {
		kvstore_free(node);
		return -1;
	}
	
	memset(node->key, 0, strlen(key) + 1);
	strcpy(node->key, key);
	
	node->value = kvstore_malloc(strlen(value) + 1);
	if (node->value == NULL) {
		kvstore_free(node->key);
		kvstore_free(node);
		return -1;
	}
	memset(node->value, 0, strlen(value) + 1);
	strcpy((char *)node->value, value);

	rbtree_insert(tree, node);
	tree->count ++;

	return 0;
}

char* kvs_rbtree_get(rbtree *tree, char *key) {

	rbtree_node *node = rbtree_search(tree, key);
	if (node == tree->nil) {
		return NULL;
	}

	return node->value;
	
}


int kvs_rbtree_delete(rbtree *tree, char *key) {

	rbtree_node *node = rbtree_search(tree, key);
	if (node == tree->nil) {
		return -1;
	}
	
	rbtree_node *cur = rbtree_delete(tree, node);

	if (!cur) {
		kvstore_free(cur->key);
		kvstore_free(cur->value);
		kvstore_free(cur);
	}
	tree->count --;
	
	return 0;
}


int kvs_rbtree_modify(rbtree *tree, char *key, char *value) {

	rbtree_node *node = rbtree_search(tree, key);
	if (node == tree->nil) {
		return -1;
	}

	char *tmp = node->value;
	kvstore_free(tmp);
	
	node->value = kvstore_malloc(strlen(value) + 1);
	if (node->value == NULL) {
		return -1;
	}
	strcpy(node->value, value);

	return 0;
}

int kvs_rbtree_count(rbtree *tree) {

	return tree->count;

}


rbtree Tree;







#if 0


int main() {
#if ENABLE_KEY_CHAR

	KEY_TYPE keyArray[10] = {
		"King", "Darren", "Mark", "Vico", "Nick", "cb", "1323", "Zvoice", "LingSheng", "Youzi"
	};

	char *valueArray[10] = {
		"king", "darren", "mark", "vico", "nick", "cb", "1323", "zvoce", "lingsheng", "youzi"
	};

	rbtree *T = (rbtree *)malloc(sizeof(rbtree));
	if (T == NULL) {
		printf("malloc failed\n");
		return -1;
	}

	T->nil = (rbtree_node*)malloc(sizeof(rbtree_node));
	T->nil->key = malloc(1);
	*(T->nil->key) = '\0';
	
	
	T->nil->color = BLACK;
	T->root = T->nil;

	rbtree_node *node = T->nil;

	int i = 0;
	for (i = 0;i < 10;i ++) {
		node = (rbtree_node*)malloc(sizeof(rbtree_node));
		
		node->key = malloc(strlen(keyArray[i]) + 1);
		memset(node->key, 0, strlen(keyArray[i]) + 1);
		strcpy(node->key, keyArray[i]);
		
		node->value = malloc(strlen(valueArray[i]) + 1);
		memset(node->value, 0, strlen(valueArray[i]) + 1);
		strcpy((char *)node->value, keyArray[i]);

		rbtree_insert(T, node);

	}

	rbtree_traversal(T, T->root);
	printf("----------------------------------------\n");

	for (i = 0;i < 10;i ++) {

		rbtree_node *node = rbtree_search(T, keyArray[i]);
		rbtree_node *cur = rbtree_delete(T, node);

		if (!cur) {
			free(cur->key);
			free(cur->value);
			free(cur);
		}

		rbtree_traversal(T, T->root);
		printf("----------------------------------------\n");
	}

#else
	int keyArray[20] = {24,25,13,35,23, 26,67,47,38,98, 20,19,17,49,12, 21,9,18,14,15};

	rbtree *T = (rbtree *)malloc(sizeof(rbtree));
	if (T == NULL) {
		printf("malloc failed\n");
		return -1;
	}
	
	T->nil = (rbtree_node*)malloc(sizeof(rbtree_node));
	T->nil->color = BLACK;
	T->root = T->nil;

	rbtree_node *node = T->nil;
	int i = 0;
	for (i = 0;i < 20;i ++) {
		node = (rbtree_node*)malloc(sizeof(rbtree_node));
		node->key = keyArray[i];
		node->value = NULL;

		rbtree_insert(T, node);
		
	}

	rbtree_traversal(T, T->root);
	printf("----------------------------------------\n");

	for (i = 0;i < 20;i ++) {

		rbtree_node *node = rbtree_search(T, keyArray[i]);
		rbtree_node *cur = rbtree_delete(T, node);
		free(cur);

		rbtree_traversal(T, T->root);
		printf("----------------------------------------\n");
	}
	
#endif

	
}

#endif


