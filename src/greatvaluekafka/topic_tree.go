package greatvaluekafka

type TopicTreeNode struct {
	// the topic name
	Name string

	// the reference to the TopicTreeNode
	Topic *Topic

	// the children node for the topic
	Children []*TopicTreeNode

	//
}

// NewTopicTreeNode creates a new topic tree node
func NewTopicTreeNode(name string) *TopicTreeNode {
	return &TopicTreeNode{
		Name:     name,
		Topic:    nil,
		Children: make([]*TopicTreeNode, 0),
	}
}

// Create a new topic tree node based on the path provided
func (t *TopicTreeNode) Create(names []string, index int) *TopicTreeNode {
	// Check if this is the last name in the array
	if index == len(names) {
		return t
	}

	// Check if the node with the same name already exists as a child
	for _, child := range t.Children {
		if child.Name == names[index] {
			return child.Create(names, index+1)
		}
	}

	// Create a new child with the name
	child := NewTopicTreeNode(names[index])
	t.AddChild(child)

	return child.Create(names, index+1)
}

func (t *TopicTreeNode) AddChild(child *TopicTreeNode) {
	t.Children = append(t.Children, child)
}

// search for a topic in the tree
// if the topic is found, return the node
// if the topic is not found, return nil
// TODO: Confirm this logic
func (t *TopicTreeNode) Search(names []string, index int) *TopicTreeNode {
	// Check if this is the last name in the array and we have found the topic
	if index == len(names) {
		return t
	}

	// Check if the current node has a child with the name required
	for _, child := range t.Children {
		if child.Name == names[index] {
			return child.Search(names, index+1)
		}
	}

	return nil
}

// GetLeafNodes returns all the leaf nodes in the subtree rooted at this node.
func (t *TopicTreeNode) GetLeafNodes() []*TopicTreeNode {
	if len(t.Children) == 0 {
		return []*TopicTreeNode{t}
	}

	leafNodes := make([]*TopicTreeNode, 0)
	for _, child := range t.Children {
		leafNodes = append(leafNodes, child.GetLeafNodes()...)
	}
	return leafNodes
}
