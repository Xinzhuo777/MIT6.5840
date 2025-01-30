package main

import "fmt"

func sayHello(name string) string {
    return fmt.Sprintf("Hello, %s!", name)
}

func main() {
    message := sayHello("World")
    fmt.Println(message)
}