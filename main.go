package main

import (
	"bufio"
	"fmt"
	"os"
	"runtime"
	"sync"
)

func readFile(wg *sync.WaitGroup, r chan<- []byte) {
	defer wg.Done()
	f, err := os.Open("articles.json")

	if err != nil {
		fmt.Println("Occured an error...", err.Error())
	}

	defer f.Close()

	sc := bufio.NewScanner(f)

	for sc.Scan() {
		r <- sc.Bytes()
	}
}

func main() {
	runtime.GOMAXPROCS(2)
	var wg sync.WaitGroup
	data := make(chan []byte, 5000)

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go readFile(&wg, data)
	}

	go func() {
		wg.Wait()
		close(data)
	}()

	for bt := range data {
		ProduceMessage(&wg, bt)
	}
}
