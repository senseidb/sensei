package com.senseidb.bql.parsers;

import org.antlr.v4.runtime.tree.ParseTree;

/**
 *
 * @author Sam Harwell
 */
public class SemanticException extends Exception {
  /**
  *
  */
  private static final long serialVersionUID = 1L;
  private final ParseTree node;

  public SemanticException(ParseTree node, String message) {
    super(message);
    this.node = node;
  }

  public SemanticException(ParseTree node, String message, Throwable cause) {
    super(message, cause);
    this.node = node;
  }

  public ParseTree getNode() {
    return node;
  }
}
