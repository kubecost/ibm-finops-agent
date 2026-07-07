package nodes

// NodeCollectionResult captures the outcome of a node-stats collection attempt for a single node.
type NodeCollectionResult struct {
	NodeName string
	Method   string // "direct", "proxy", or "direct+proxy"
	Success  bool
	Error    error
}
