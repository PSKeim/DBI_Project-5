#include "QueryTree.h"
#include "QueryTreeNode.h"

QueryTree::QueryTree(){

  root = NULL;

}

QueryTree::~QueryTree(){

  if (root){
    delete root;
  }

}

void QueryTree::PrintTree() {
  PrintTreeInorder(root);
}

void QueryTree::PrintTreeInorder(QueryTreeNode *node) {

  if (node->left){
    PrintTreeInorder(node->left);
  }

  node->PrintNode();

  if (node->right){
    PrintTreeInorder(node->right);
  }

} // end PrintTreeInorder

void QueryTree::PrintTreeCountPipes() {
  int i = 0;
  PrintTreeInorderCountPipes(root, i);
}

void QueryTree::PrintTreeInorderCountPipes(QueryTreeNode *node, int &i) {

  if (node->left){
    PrintTreeInorderCountPipes(node->left, i);
  }

  node->PrintNode();

    // Print out first input pipe ID
  cout << "Input pipe ID: " << i << endl;
  i++;

    // If node is a join, print out pipe ID for second pipe
  if (node->type == JOIN){
    cout << "Input pipe ID: " << i+1 << endl;
    i++;
  }

    // Print out ID of output pipe
  cout << "Output pipe ID: " << i << "\n" << endl;
  i++;

  if (node->right){
    PrintTreeInorderCountPipes(node->right, i);
  }

} // end PrintTreeInorder
