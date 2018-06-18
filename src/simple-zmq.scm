(define-module (simple-zmq)
 #:use-module (system foreign)
 #:use-module (rnrs bytevectors)
 #:use-module (ice-9 iconv)
 #:export (zmq-get-buffer-size
	   zmq-set-buffer-size
	   zmq-create-context
	   zmq-destroy-context
	   zmq-create-socket
	   zmq-close-socket
	   zmq-bind-socket
	   zmq-unbind-socket
	   zmq-connect
	   zmq-msg-init
	   zmq-get-socket-option
	   zmq-message-send
	   zmq-message-receive	   
	   zmq-receive
	   zmq-send
	   zmq-message-content
	   zmq-get-msg-parts
	   zmq-send-msg-parts
           zmq-set-socket-option))

(define BUF-SIZE 4096)

(define zmq (dynamic-link "libzmq"))

(define (get-func-pointer func-name) (dynamic-func func-name zmq))
(define zmq_strerror   (pointer->procedure '*  (get-func-pointer "zmq_strerror")   '()))
(define zmq_ctx_new    (pointer->procedure '*  (get-func-pointer "zmq_ctx_new")    '()))
(define zmq_ctx_term   (pointer->procedure int (get-func-pointer "zmq_ctx_term")   (list '*)))
(define zmq_socket     (pointer->procedure '*  (get-func-pointer "zmq_socket")     (list '* int)))
(define zmq_close      (pointer->procedure int (get-func-pointer "zmq_close")      (list '*)))
(define zmq_bind       (pointer->procedure int (get-func-pointer "zmq_bind")       (list '* '*)))
(define zmq_unbind     (pointer->procedure int (get-func-pointer "zmq_unbind")     (list '* '*)))
(define zmq_connect    (pointer->procedure int (get-func-pointer "zmq_connect")     (list '* '*)))
(define zmq_msg_init   (pointer->procedure int (get-func-pointer "zmq_msg_init")   (list '*)))
(define zmq_msg_send   (pointer->procedure int (get-func-pointer "zmq_msg_send")   (list '* '* int)))
(define zmq_recv       (pointer->procedure int (get-func-pointer "zmq_recv")       (list '* '* size_t int)))
(define zmq_send       (pointer->procedure int (get-func-pointer "zmq_send")       (list '* '* size_t int)))
(define zmq_msg_recv   (pointer->procedure int (get-func-pointer "zmq_msg_recv")   (list '* '* int)))
(define zmq_msg_data   (pointer->procedure '*  (get-func-pointer "zmq_msg_data")   (list '*)))
(define zmq_getsockopt (pointer->procedure int (get-func-pointer "zmq_getsockopt") (list '* int '* '*)))
(define zmq_setsockopt (pointer->procedure int (get-func-pointer "zmq_setsockopt") (list '* int '* size_t)))

;; socket types 
(define zmq-socket-types
  '((ZMQ_PAIR   0)
    (ZMQ_PUB    1)
    (ZMQ_SUB    2)
    (ZMQ_REQ    3)
    (ZMQ_REP    4)
    (ZMQ_DEALER 5)
    (ZMQ_ROUTER 6)
    (ZMQ_PULL   7)
    (ZMQ_PUSH   8)
    (ZMQ_XPUB   9)
    (ZMQ_XSUB   10)
    (ZMQ_STREAM 11)))

(define (zmq-get-buffer-size)
  BUF-SIZE)

(define (zmq-set-buffer-size new-size)
  "Change a buffer size, affects the zmq-get-msg-parts function"
  (set! BUF-SIZE new-size))

(define (zmq-get-socket-type type)
  (let ((result (assq type zmq-socket-types)))
    (if result (cadr result) (error "Invalid socket type" ""))))

(define (zmq-get-error message)
  (error message (pointer->string (zmq_strerror))))

(define (zmq-create-context)
  (let ((context (zmq_ctx_new)))
    (if (null-pointer? context)
	(zmq-get-error "Impossible to create context")
	context)))

(define (zmq-destroy-context context)
  (let ((result (zmq_ctx_term context)))
    (if (not (= result 0))
	(zmq-get-error "Impossible to terminate context"))))

(define (zmq-create-socket context type)
  (let ((socket (zmq_socket context (zmq-get-socket-type type))))
    (if (null-pointer? socket)
	(zmq-get-error "Impossible to create socket")
	socket)))

(define (zmq-close-socket socket)
  (let ((result (zmq_close socket)))
    (if (not (= result 0))
	(zmq-get-error "Impossible to close socket"))))

(define (zmq-bind-socket socket address)
  (let ((result (zmq_bind socket (string->pointer address))))
    (if (not (= result 0))
	(zmq-get-error "Impossible to bind socket"))))

(define (zmq-unbind-socket socket address)
  (let ((result (zmq_unbind socket (string->pointer address))))
    (if (not (= result 0))
	(zmq-get-error "Impossible to unbind socket"))))

(define (zmq-connect socket address)
  (let ((result (zmq_connect socket (string->pointer address))))
    (if (not (= result 0))
	(zmq-get-error "Impossible to connect to socket"))))

(define (zmq-msg-init)
  (let ((zmq-msg-ptr (bytevector->pointer (make-bytevector 40))))
    (let ((result (zmq_msg_init zmq-msg-ptr)))
      (if (not (= result 0))
	  (zmq-get-error "Impossible to initialize zmq message")
	  zmq-msg-ptr))))

(define (zmq-get-socket-option socket option)
  (let ((opt (make-bytevector 8))
	(size (make-bytevector 8)))
    (bytevector-u8-set! size 0 8)
    (let ((result (zmq_getsockopt socket option (bytevector->pointer opt) (bytevector->pointer size) )))
      (if (= result -1)
	  (zmq-get-error "Impossible to get socket option")
	  (bytevector-u8-ref opt 0)))))

(define (zmq-set-socket-option socket option str)
  (let ((vstr (string->bytevector str "ASCII"))
        (lstr (string-length str)))
    (let ((result (zmq_setsockopt socket option (bytevector->pointer vstr) lstr)))
      (if (= result -1)
	  (zmq-get-error "Impossible to set socket option")))))

(define (zmq-message-send socket message)
  (let ((result (zmq_msg_send message socket 0)))
    (if (= result -1)
	(zmq-get-error "Impossible to send message"))))

(define (zmq-receive socket len)
  (let  ((buffer (make-bytevector len 0)))
    (let ((result (zmq_recv socket (bytevector->pointer buffer) len 0)))
      (if (< result 0)
	  (zmq-get-error "Impossible to receive message")
	  (if (> result 0)
	      (pointer->string (bytevector->pointer buffer) (min len result))
	      "")))))

(define* (zmq-send socket data #:optional (flag 0))
  (let*  ((len (string-length data))
	  (buffer (string->bytevector data "ASCII")))
    (let ((result (zmq_send socket (bytevector->pointer buffer) len flag)))
      (if (< result 0)
	  (zmq-get-error "Impossible to send message")
	  ))))

(define (zmq-message-receive socket message)
  (let  ((result (zmq_msg_recv message socket 0)))
    (if (= result -1)
	(zmq-get-error "Impossible to receive message")
	message)))

(define (zmq-message-content message)
  (let ((content-ptr (zmq_msg_data message)))
    (if (null-pointer? content-ptr)
	(zmq-get-error "Impossible to get message data")
	content-ptr)))

(define* (zmq-get-msg-parts socket #:optional (parts '()))
  (let ((part (string-copy (zmq-receive socket BUF-SIZE))))
    (let ((new-parts (append parts (list part))))
      (let ((opt (zmq-get-socket-option socket 13))) ;; 13 ZMQ_RCVMORE
	(if (> opt 0)
	    (zmq-get-msg-parts socket new-parts)
	    new-parts)))))

(define (zmq-send-msg-parts socket parts)
  (if (not (null? parts))
      (let* ((data (car parts))
	     (flag  (if (> (length parts) 1) 2 0))) ;; 2 ZMQ_SNDMORE
	(zmq-send socket data flag)
	(zmq-send-msg-parts socket (cdr parts))
	)))
