# ZNode2Flow
ZNode2Flow is a Java library for watching a ZooKeeper node via a reactive stream. 

# WIP
This project is a work in progress.

# Usage
## Subscribing
To subscribe to the stream:
```
Flow.Publisher<byte[]> publisher = ZNodePublisher.withCachedCurator(myZkConnection, myZkPath);
publisher.subscribe(mySubscriber);
```

Your subscriber will now receive the values of the ZNode as they change subject to the guarantees
provided by ZooKeeper watches (it is possible to miss intermediary values when they are changing frequently).

## Submitting
To submit to the stream:
```
ZNodePublisher zNode = ZNodePublisher.withCachedCurator(myZkConnection, myZkPath);
zNode.submit(mySubmission);
```

Submitting a value to the publisher will write the value to the corresponding ZNode. Any watchers of that ZNode
will then see the new value.

## Building
### Build
`./gradlew build`

### Publish to local Maven repo
`./gradlew publishToMavenLocal`

## Testing
`./gradlew test`

## Depending on this library
Once you have published to your local maven repo you can depend on this library via the following:
```
group id: net.mattixtech
artifact id: znode2flow
```