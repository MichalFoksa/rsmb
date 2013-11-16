/*******************************************************************************
 * Copyright (c) 2011, 2013 IBM Corp.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Eclipse Distribution License v1.0 which accompany this distribution. 
 *
 * The Eclipse Public License is available at 
 *    http://www.eclipse.org/legal/epl-v10.html
 * and the Eclipse Distribution License is available at 
 *   http://www.eclipse.org/org/documents/edl-v10.php.
 *
 * Contributors:
 *    Ian Craggs - initial API and implementation and/or initial documentation
 *******************************************************************************/


#if !defined(TREE_H)
#define TREE_H

/*BE
map TREE_BITS
{
	"heap_tracking" 1 : .
	"allow_duplicates" 2 : .
}
 
defm defTree(T) // macro to define a tree

def T concat Node
{
	n32 ptr T concat Node "parent"
	n32 ptr T concat Node "left"
	n32 ptr T concat Node "right"
	n32 ptr T id2str(T)
	n32 suppress "size"
	n32 dec suppress "red"
}

def T concat TreeIndex
{
	n32 ptr T concat Node suppress "root"
	n32 ptr DATA suppress "compare"
}

def T concat Tree
{
	2 T concat TreeIndex "index"
	n32 dec "indexes"
	n32 dec "count"
	n32 dec suppress "size"
	n32 map TREE_BITS "bits"
}

endm

defTree(INT)
defTree(STRING)
defTree(TMP)

BE*/

/**
 * Structure to hold all data for one list element
 */
typedef struct NodeStruct
{
	struct NodeStruct *parent,   /**< pointer to parent tree node, in case we need it */
					  *child[2]; /**< pointers to child tree nodes 0 = left, 1 = right */
	void* content;				 /**< pointer to element content */
	int size;					 /**< size of content */
	unsigned int red : 1;
} Node;


/**
 * Structure to hold all data for one tree
 */
typedef struct
{
	struct
	{
		Node *root;	/**< root node pointer */
		int (*compare)(void*, void*, int); /**< comparison function */
	} index[2];
	int indexes, /**< no of indexes into tree */
		count,  /**< no of items */
		size;  /**< heap storage used */
	unsigned int heap_tracking : 1; /**< switch on heap tracking for this tree? */
	unsigned int allow_duplicates : 1; /**< switch to allow duplicate entries */
} Tree;


Tree* TreeInitialize(int(*compare)(void*, void*, int));
void TreeInitializeNoMalloc(Tree* aTree, int(*compare)(void*, void*, int));
void TreeAddIndex(Tree* aTree, int(*compare)(void*, void*, int));

void* TreeAdd(Tree* aTree, void* content, int size);

void* TreeRemove(Tree* aTree, void* content);

void* TreeRemoveKey(Tree* aTree, void* key);
void* TreeRemoveKeyIndex(Tree* aTree, void* key, int index);

void* TreeRemoveNodeIndex(Tree* aTree, Node* aNode, int index);

void TreeFree(Tree* aTree);

Node* TreeFind(Tree* aTree, void* key);
Node* TreeFindIndex(Tree* aTree, void* key, int index);

Node* TreeNextElement(Tree* aTree, Node* curnode);

int TreeIntCompare(void* a, void* b, int);
int TreePtrCompare(void* a, void* b, int);
int TreeStringCompare(void* a, void* b, int);

#endif
