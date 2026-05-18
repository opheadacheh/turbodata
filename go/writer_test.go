package turbodata

// func TestWriter(t *testing.T) {
// 	buf := bytes.NewBuffer(nil)
// 	writer := NewWriter(buf)
// 	writer.OpenTopics([]string{"test"}, []map[string]any{{"foo": "bar"}})
// 	for i := 0; i < 100; i++ {
// 		writer.WriteMessage("test", []byte("test"), int64(i))
// 	}
// 	writer.CloseTopic()
// 	writer.Close()

// 	reader, err := NewReader(bytes.NewReader(buf.Bytes()))
// 	if err != nil {
// 		t.Fatalf("failed to create reader: %v", err)
// 	}

// 	_, err = reader.Summary()
// 	// Print(summary)
// }
