// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: Commit.proto

package org.apache.hadoop.hbase.protobuf.generated;

public final class DistributedCommitProtos {
  private DistributedCommitProtos() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
  }
  public enum CommitPhase
      implements com.google.protobuf.ProtocolMessageEnum {
    PRE_PREPARE(0, 1),
    PREPARE(1, 2),
    PRE_COMMIT(2, 3),
    COMMIT(3, 4),
    POST_COMMIT(4, 5),
    ;
    
    public static final int PRE_PREPARE_VALUE = 1;
    public static final int PREPARE_VALUE = 2;
    public static final int PRE_COMMIT_VALUE = 3;
    public static final int COMMIT_VALUE = 4;
    public static final int POST_COMMIT_VALUE = 5;
    
    
    public final int getNumber() { return value; }
    
    public static CommitPhase valueOf(int value) {
      switch (value) {
        case 1: return PRE_PREPARE;
        case 2: return PREPARE;
        case 3: return PRE_COMMIT;
        case 4: return COMMIT;
        case 5: return POST_COMMIT;
        default: return null;
      }
    }
    
    public static com.google.protobuf.Internal.EnumLiteMap<CommitPhase>
        internalGetValueMap() {
      return internalValueMap;
    }
    private static com.google.protobuf.Internal.EnumLiteMap<CommitPhase>
        internalValueMap =
          new com.google.protobuf.Internal.EnumLiteMap<CommitPhase>() {
            public CommitPhase findValueByNumber(int number) {
              return CommitPhase.valueOf(number);
            }
          };
    
    public final com.google.protobuf.Descriptors.EnumValueDescriptor
        getValueDescriptor() {
      return getDescriptor().getValues().get(index);
    }
    public final com.google.protobuf.Descriptors.EnumDescriptor
        getDescriptorForType() {
      return getDescriptor();
    }
    public static final com.google.protobuf.Descriptors.EnumDescriptor
        getDescriptor() {
      return org.apache.hadoop.hbase.protobuf.generated.DistributedCommitProtos.getDescriptor().getEnumTypes().get(0);
    }
    
    private static final CommitPhase[] VALUES = {
      PRE_PREPARE, PREPARE, PRE_COMMIT, COMMIT, POST_COMMIT, 
    };
    
    public static CommitPhase valueOf(
        com.google.protobuf.Descriptors.EnumValueDescriptor desc) {
      if (desc.getType() != getDescriptor()) {
        throw new java.lang.IllegalArgumentException(
          "EnumValueDescriptor is not for this type.");
      }
      return VALUES[desc.getIndex()];
    }
    
    private final int index;
    private final int value;
    
    private CommitPhase(int index, int value) {
      this.index = index;
      this.value = value;
    }
    
    // @@protoc_insertion_point(enum_scope:CommitPhase)
  }
  
  
  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    java.lang.String[] descriptorData = {
      "\n\014Commit.proto*X\n\013CommitPhase\022\017\n\013PRE_PRE" +
      "PARE\020\001\022\013\n\007PREPARE\020\002\022\016\n\nPRE_COMMIT\020\003\022\n\n\006C" +
      "OMMIT\020\004\022\017\n\013POST_COMMIT\020\005BJ\n*org.apache.h" +
      "adoop.hbase.protobuf.generatedB\027Distribu" +
      "tedCommitProtosH\001\240\001\001"
    };
    com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner assigner =
      new com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner() {
        public com.google.protobuf.ExtensionRegistry assignDescriptors(
            com.google.protobuf.Descriptors.FileDescriptor root) {
          descriptor = root;
          return null;
        }
      };
    com.google.protobuf.Descriptors.FileDescriptor
      .internalBuildGeneratedFileFrom(descriptorData,
        new com.google.protobuf.Descriptors.FileDescriptor[] {
        }, assigner);
  }
  
  // @@protoc_insertion_point(outer_class_scope)
}
