# ZNode2Flow
ZNode2Flow is a Java library for watching a ZooKeeper node via a reactive stream. 

# Usage
To subscribe to the stream:
```
Flow.Publisher<byte[]> publisher = new FlowingZNodeIT(FlowingZNodeIT.startedCurator(myZkConnection), myZkPath, myZkLockPath);
publisher.subscribe(mySubscriber);
```

To submit to the stream:
```
FlowingZNodeIT zNode = new FlowingZNodeIT(FlowingZNodeIT.startedCurator(myZkConnection), myZkPath, myZkLockPath);
zNode.submit(mySubmission);
```

## Building
### With Tests
`./gradlew build`
### Without Tests
`./gradlew build -x test`
### Publish to local Maven repo
`./gradlew publishToMavenLocal`

## Testing
`./gradlew test`