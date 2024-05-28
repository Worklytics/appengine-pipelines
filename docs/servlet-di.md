

Issues:
  - servlets theoretically serializable
  - Dagger can't inject private fields
  - Not nice to have pkg-private fields for dependencies


Current approach:
```java

@AllArgsConstructor(onConstructor_ = @Inject)
class MyServlet extends HttpServlet { 
    
    private final Dependency dependency;
}
```


But may need:
```java
@NoArgsConstructor
class MyServlet extends HttpServlet {
    
    @Inject
    private transient Dependency dependency;
}
```

Or perhaps better, override `readResolve()` and take a singleton from DI on deserialization, which
shouldn't happen.
https://www.digitalocean.com/community/tutorials/java-singleton-design-pattern-best-practices-examples#8-serialization-and-singleton
