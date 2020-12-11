;;; Guile-Simple-ZMQ --- ZeroMQ bindings for GNU Guile.
;;; Copyright © 2018 Evgeny Panfilov <epanfilov@gmail.com>
;;; Copyright © 2018 Pierre-Antoine Rouby <contact@parouby.fr>
;;; Copyright © 2018, 2019 Ludovic Courtès <ludo@gnu.org>
;;;
;;; This file is part of Guile-Simple-ZMQ.
;;;
;;; Guile-Simple-ZMQ is free software; you can redistribute it and/or modify it
;;; under the terms of the GNU General Public License as published by
;;; the Free Software Foundation; either version 3 of the License, or (at
;;; your option) any later version.
;;;
;;; Guile-Simple-ZMQ is distributed in the hope that it will be useful, but
;;; WITHOUT ANY WARRANTY; without even the implied warranty of
;;; MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;;; GNU General Public License for more details.
;;;
;;; You should have received a copy of the GNU General Public License
;;; along with Guile-Simple-ZMQ.  If not, see <http://www.gnu.org/licenses/>.

(define-module (simple-zmq)
  #:use-module (system foreign)
  #:use-module (rnrs bytevectors)
  #:use-module (srfi srfi-9)
  #:use-module (srfi srfi-11)
  #:use-module (ice-9 iconv)
  #:use-module (ice-9 match)
  #:export (zmq-context?
            zmq-socket?

            zmq-get-buffer-size
            zmq-set-buffer-size
            zmq-get-msg-encoding
            zmq-set-msg-encoding
            zmq-create-context
            zmq-destroy-context
            zmq-set-context-option
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
            zmq-receive-bytevector
            zmq-send-bytevector
            zmq-message-content
            zmq-get-msg-parts
            zmq-send-msg-parts
            zmq-get-msg-parts-bytevector
            zmq-send-msg-parts-bytevector
            zmq-set-socket-option
            string->bv
            bv->string
            string-list->bv-list
            bv-list->string-list

            poll-item?
            poll-item
            poll-item-socket
            poll-item-fd
            poll-item-events
            zmq-poll

            zmq-proxy

            ZMQ_PAIR
            ZMQ_PUB
            ZMQ_SUB
            ZMQ_REQ
            ZMQ_REP
            ZMQ_DEALER
            ZMQ_ROUTER
            ZMQ_PULL
            ZMQ_PUSH
            ZMQ_XPUB
            ZMQ_XSUB
            ZMQ_STREAM

            ZMQ_IO_THREADS
            ZMQ_MAX_SOCKETS

            ZMQ_AFFINITY                 
            ZMQ_IDENTITY                 
            ZMQ_SUBSCRIBE                
            ZMQ_UNSUBSCRIBE              
            ZMQ_RATE                     
            ZMQ_RECOVERY_IVL             
            ZMQ_SNDBUF
            ZMQ_RCVBUF
            ZMQ_RCVMORE
            ZMQ_FD
            ZMQ_EVENTS
            ZMQ_TYPE
            ZMQ_LINGER
            ZMQ_RECONNECT_IVL
            ZMQ_BACKLOG
            ZMQ_RECONNECT_IVL_MAX
            ZMQ_MAXMSGSIZE
            ZMQ_SNDHWM
            ZMQ_RCVHWM
            ZMQ_MULTICAST_HOPS
            ZMQ_RCVTIMEO
            ZMQ_SNDTIMEO
            ZMQ_LAST_ENDPOINT
            ZMQ_ROUTER_MANDATORY
            ZMQ_TCP_KEEPALIVE
            ZMQ_TCP_KEEPALIVE_CNT
            ZMQ_TCP_KEEPALIVE_IDLE
            ZMQ_TCP_KEEPALIVE_INTVL
            ZMQ_IMMEDIATE
            ZMQ_XPUB_VERBOSE
            ZMQ_ROUTER_RAW
            ZMQ_IPV6
            ZMQ_MECHANISM
            ZMQ_PLAIN_SERVER
            ZMQ_PLAIN_USERNAME
            ZMQ_PLAIN_PASSWORD
            ZMQ_CURVE_SERVER
            ZMQ_CURVE_PUBLICKEY
            ZMQ_CURVE_SECRETKEY
            ZMQ_CURVE_SERVERKEY
            ZMQ_PROBE_ROUTER
            ZMQ_REQ_CORRELATE
            ZMQ_REQ_RELAXED
            ZMQ_CONFLATE
            ZMQ_ZAP_DOMAIN
            ZMQ_ROUTER_HANDOVER
            ZMQ_TOS
            ZMQ_CONNECT_RID
            ZMQ_GSSAPI_SERVER
            ZMQ_GSSAPI_PRINCIPAL
            ZMQ_GSSAPI_SERVICE_PRINCIPAL
            ZMQ_GSSAPI_PLAINTEXT
            ZMQ_HANDSHAKE_IVL
            ZMQ_SOCKS_PROXY
            ZMQ_XPUB_NODROP
            ZMQ_BLOCKY
            ZMQ_XPUB_MANUAL
            ZMQ_XPUB_WELCOME_MSG
            ZMQ_STREAM_NOTIFY
            ZMQ_INVERT_MATCHING
            ZMQ_HEARTBEAT_IVL
            ZMQ_HEARTBEAT_TTL
            ZMQ_HEARTBEAT_TIMEOUT
            ZMQ_XPUB_VERBOSER
            ZMQ_CONNECT_TIMEOUT
            ZMQ_TCP_MAXRT
            ZMQ_THREAD_SAFE
            ZMQ_MULTICAST_MAXTPDU
            ZMQ_VMCI_BUFFER_SIZE
            ZMQ_VMCI_BUFFER_MIN_SIZE
            ZMQ_VMCI_BUFFER_MAX_SIZE
            ZMQ_VMCI_CONNECT_TIMEOUT
            ZMQ_USE_FD

            ZMQ_MORE
            ZMQ_SHARED

            ZMQ_DONTWAIT
            ZMQ_SNDMORE

            ZMQ_POLLIN
            ZMQ_POLLOUT
            ZMQ_POLLERR))

(define BUF-SIZE 4096)
(define MSG-ENCODING "UTF8")

(define zmq (dynamic-link "libzmq"))

(define (import-func ret-type name arguments errno?)
  (catch #t
    (lambda ()
      (let ((ptr (dynamic-func name zmq)))
        (pointer->procedure ret-type ptr arguments #:return-errno? errno?)))
    (lambda args
      (lambda _
        (error (format #f "~a: import-func failed: ~s"
                       name args))))))

(define zmq_bind       (import-func int "zmq_bind"       (list '* '*) #t))
(define zmq_close      (import-func int "zmq_close"      (list '*)    #t))
(define zmq_connect    (import-func int "zmq_connect"    (list '* '*) #t))
(define zmq_ctx_new    (import-func '*  "zmq_ctx_new"    '()          #t))
(define zmq_ctx_term   (import-func int "zmq_ctx_term"   (list '*)    #t))
(define zmq_ctx_set    (import-func int "zmq_ctx_set"    (list '* int int) #t))
(define zmq_getsockopt (import-func int "zmq_getsockopt" (list '* int '* '*) #t))
(define zmq_msg_data   (import-func '*  "zmq_msg_data"   (list '*)           #f))
(define zmq_msg_init   (import-func int "zmq_msg_init"   (list '*)           #f))
(define zmq_msg_recv   (import-func int "zmq_msg_recv"   (list '* '* int)    #t))
(define zmq_msg_send   (import-func int "zmq_msg_send"   (list '* '* int)    #t))
(define zmq_recv       (import-func int "zmq_recv"       (list '* '* size_t int) #t))
(define zmq_send       (import-func int "zmq_send"       (list '* '* size_t int) #t))
(define zmq_setsockopt (import-func int "zmq_setsockopt" (list '* int '* size_t) #t))
(define zmq_socket     (import-func '*  "zmq_socket"     (list '* int) #t))
(define zmq_strerror   (import-func '*  "zmq_strerror"   (list int)    #f))
(define zmq_unbind     (import-func int "zmq_unbind"     (list '* '*)  #t))
(define zmq_poll       (import-func int "zmq_poll"       (list '* int long) #t))
(define zmq_proxy      (import-func int "zmq_proxy"      (list '* '* '*) #t))

;; Data types.

(define-wrapped-pointer-type <zmq-context> zmq-context?
  pointer->context context->pointer
  (lambda (context port)
    (format port "#<zmq-context ~a>"
            (number->string (object-address context) 16))))

(define-wrapped-pointer-type <zmq-socket> zmq-socket?
  pointer->socket socket->pointer
  (lambda (socket port)
    (let ((type (zmq-get-socket-option socket ZMQ_TYPE)))
      (format port "#<zmq-socket type: ~a ~a>"
              type
              (number->string (object-address socket) 16)))))

;; Structure used by 'zmq-poll'.
(define-record-type <poll-item>
  (%poll-item socket fd events)
  poll-item?
  (socket   poll-item-socket)
  (fd       poll-item-fd)
  (events   poll-item-events))

(define* (poll-item socket
                    #:optional
                    (events (logior ZMQ_POLLIN ZMQ_POLLOUT ZMQ_POLLERR))
                    #:key (fd -1))
  "Return a new \"poll item\" record suitable for 'zmq-poll'.  The
poll item will allow 'zmq-poll' to wait for EVENTS on SOCKET or on FD if
SOCKET is #f.  EVENTS must be a bitwise-or of the ZMQ_POLL* constants."
  (%poll-item socket fd events))

;; socket types
(define ZMQ_PAIR   0)
(define ZMQ_PUB    1)
(define ZMQ_SUB    2)
(define ZMQ_REQ    3)
(define ZMQ_REP    4)
(define ZMQ_DEALER 5)
(define ZMQ_ROUTER 6)
(define ZMQ_PULL   7)
(define ZMQ_PUSH   8)
(define ZMQ_XPUB   9)
(define ZMQ_XSUB   10)
(define ZMQ_STREAM 11)

;; context options
(define ZMQ_IO_THREADS  1)
(define ZMQ_MAX_SOCKETS 2)

;; socket options
(define ZMQ_AFFINITY                 4)
(define ZMQ_IDENTITY                 5)
(define ZMQ_SUBSCRIBE                6)
(define ZMQ_UNSUBSCRIBE              7)
(define ZMQ_RATE                     8)
(define ZMQ_RECOVERY_IVL             9)
(define ZMQ_SNDBUF                   11)
(define ZMQ_RCVBUF                   12)
(define ZMQ_RCVMORE                  13)
(define ZMQ_FD                       14)
(define ZMQ_EVENTS                   15)
(define ZMQ_TYPE                     16)
(define ZMQ_LINGER                   17)
(define ZMQ_RECONNECT_IVL            18)
(define ZMQ_BACKLOG                  19)
(define ZMQ_RECONNECT_IVL_MAX        21)
(define ZMQ_MAXMSGSIZE               22)
(define ZMQ_SNDHWM                   23)
(define ZMQ_RCVHWM                   24)
(define ZMQ_MULTICAST_HOPS           25)
(define ZMQ_RCVTIMEO                 27)
(define ZMQ_SNDTIMEO                 28)
(define ZMQ_LAST_ENDPOINT            32)
(define ZMQ_ROUTER_MANDATORY         33)
(define ZMQ_TCP_KEEPALIVE            34)
(define ZMQ_TCP_KEEPALIVE_CNT        35)
(define ZMQ_TCP_KEEPALIVE_IDLE       36)
(define ZMQ_TCP_KEEPALIVE_INTVL      37)
(define ZMQ_IMMEDIATE                39)
(define ZMQ_XPUB_VERBOSE             40)
(define ZMQ_ROUTER_RAW               41)
(define ZMQ_IPV6                     42)
(define ZMQ_MECHANISM                43)
(define ZMQ_PLAIN_SERVER             44)
(define ZMQ_PLAIN_USERNAME           45)
(define ZMQ_PLAIN_PASSWORD           46)
(define ZMQ_CURVE_SERVER             47)
(define ZMQ_CURVE_PUBLICKEY          48)
(define ZMQ_CURVE_SECRETKEY          49)
(define ZMQ_CURVE_SERVERKEY          50)
(define ZMQ_PROBE_ROUTER             51)
(define ZMQ_REQ_CORRELATE            52)
(define ZMQ_REQ_RELAXED              53)
(define ZMQ_CONFLATE                 54)
(define ZMQ_ZAP_DOMAIN               55)
(define ZMQ_ROUTER_HANDOVER          56)
(define ZMQ_TOS                      57)
(define ZMQ_CONNECT_RID              61)
(define ZMQ_GSSAPI_SERVER            62)
(define ZMQ_GSSAPI_PRINCIPAL         63)
(define ZMQ_GSSAPI_SERVICE_PRINCIPAL 64)
(define ZMQ_GSSAPI_PLAINTEXT         65)
(define ZMQ_HANDSHAKE_IVL            66)
(define ZMQ_SOCKS_PROXY              68)
(define ZMQ_XPUB_NODROP              69)
(define ZMQ_BLOCKY                   70)
(define ZMQ_XPUB_MANUAL              71)
(define ZMQ_XPUB_WELCOME_MSG         72)
(define ZMQ_STREAM_NOTIFY            73)
(define ZMQ_INVERT_MATCHING          74)
(define ZMQ_HEARTBEAT_IVL            75)
(define ZMQ_HEARTBEAT_TTL            76)
(define ZMQ_HEARTBEAT_TIMEOUT        77)
(define ZMQ_XPUB_VERBOSER            78)
(define ZMQ_CONNECT_TIMEOUT          79)
(define ZMQ_TCP_MAXRT                80)
(define ZMQ_THREAD_SAFE              81)
(define ZMQ_MULTICAST_MAXTPDU        84)
(define ZMQ_VMCI_BUFFER_SIZE         85)
(define ZMQ_VMCI_BUFFER_MIN_SIZE     86)
(define ZMQ_VMCI_BUFFER_MAX_SIZE     87)
(define ZMQ_VMCI_CONNECT_TIMEOUT     88)
(define ZMQ_USE_FD                   89)

;; zmq message options
(define ZMQ_MORE   1)
(define ZMQ_SHARED 3)

;; zmq send recv options
(define ZMQ_DONTWAIT 1)
(define ZMQ_SNDMORE  2)

;; polling
(define ZMQ_POLLIN  1)
(define ZMQ_POLLOUT 2)
(define ZMQ_POLLERR 4)

(define (zmq-get-buffer-size)
  BUF-SIZE)

(define (zmq-set-buffer-size new-size)
  "Change a buffer size, affects the zmq-get-msg-parts function"
  (set! BUF-SIZE new-size))

(define (zmq-get-msg-encoding)
  MSG-ENCODING)

(define (zmq-set-msg-encoding new-encoding)
  "Change encoding that used in bytevectors <-> string transformations. UTF8, ASCII etc"
  (set! MSG-ENCODING new-encoding))

;;
;; Error.
;;

(define (zmq-get-strerror errno)
  (pointer->string (zmq_strerror errno)))

(define (zmq-get-error errno)
  (let ((strerror (zmq-get-strerror errno)))
    (throw 'zmq-error errno strerror)))

(define (zmq-get-error-msg msg)
  (throw 'zmq-error `(0 ,msg)))

;;
;; ZMQ API.
;;

(define (zmq-create-context)
  (let-values (((context errno) (zmq_ctx_new)))
    (if (null-pointer? context)
	(zmq-get-error errno)
	(pointer->context context))))

(define (zmq-destroy-context context)
  (let-values (((result errno) (zmq_ctx_term (context->pointer context))))
    (if (not (= result 0))
	(zmq-get-error errno))))

(define (zmq-set-context-option context option value)
  "Set the OPTION of CONTEXT to VALUE.  OPTION must be an integer such as
'ZMQ_IO_THREADS' or 'ZMQ_MAX_SOCKETS', and VALUE must be an integer."
  (let-values (((result errno)
                (zmq_ctx_set (context->pointer context) option value)))
    (unless (zero? result)
      (zmq-get-error errno))))

(define (zmq-create-socket context type)
  (let-values (((socket errno) (zmq_socket (context->pointer context) type)))
    (if (null-pointer? socket)
        (zmq-get-error errno)
	(pointer->socket socket))))

(define (zmq-close-socket socket)
  (let-values (((result errno) (zmq_close (socket->pointer socket))))
    (if (not (= result 0))
	(zmq-get-error errno))))

(define (zmq-bind-socket socket address)
  (let-values (((result errno) (zmq_bind (socket->pointer socket)
                                         (string->pointer address))))
    (if (not (= result 0))
	(zmq-get-error errno))))

(define (zmq-unbind-socket socket address)
  (let-values (((result errno) (zmq_unbind (socket->pointer socket)
                                           (string->pointer address))))
    (if (not (= result 0))
	(zmq-get-error errno))))

(define (zmq-connect socket address)
  (let-values (((result errno) (zmq_connect (socket->pointer socket)
                                            (string->pointer address))))
    (if (not (= result 0))
	(zmq-get-error errno))))

(define (zmq-msg-init)
  (let* ((zmq-msg-ptr (bytevector->pointer (make-bytevector 40)))
         (result (zmq_msg_init zmq-msg-ptr)))
    (if (not (= result 0))
        (zmq-get-error-msg "Function zmq-msg-init failed.")
        zmq-msg-ptr)))

(define (zmq-get-socket-option socket option)
  (let* ((value-size (if (= option ZMQ_TYPE)      ;TODO: Add more types.
                         (sizeof int)
                         (sizeof size_t)))
         (opt        (make-bytevector value-size))
	 (size       (make-bytevector (sizeof size_t))))
    (bytevector-uint-set! size 0 value-size
                          (native-endianness) (sizeof size_t))
    (let-values (((result errno) (zmq_getsockopt (socket->pointer socket)
                                                 option
                                                 (bytevector->pointer opt)
                                                 (bytevector->pointer size))))
      (if (= result -1)
	  (zmq-get-error errno)
	  (bytevector-uint-ref opt 0 (native-endianness) value-size)))))

(define (zmq-set-socket-option socket option value)
  (define (value->type+length value)
    (cond
     ((string? value)                             ;for backward compatibility
      (let* ((vstr (string->bytevector value "ASCII"))
             (lstr (string-length value)))
        (values (bytevector->pointer vstr)
                lstr)))
     ((number? value)
      (let* ((bv (make-u32vector 1)))
        (bytevector-u32-native-set! bv 0 value)
        (values (bytevector->pointer bv) (sizeof uint32))))
     (else
      (zmq-get-error-msg "Wrong VALUE type in zmq-set-socket-option"))))

  (let*-values (((val len) (value->type+length value))
                ((result errno) (zmq_setsockopt (socket->pointer socket)
                                                option val len)))
    (when (= result -1)
      (zmq-get-error errno))))

(define (zmq-message-send socket message)
  (let-values (((result errno)
                (zmq_msg_send message (socket->pointer socket) 0)))
    (if (= result -1)
	(zmq-get-error errno))))

(define (zmq-receive-bytevector socket len)
  (let  ((buffer (make-bytevector len 0)))
    (let-values (((result errno) (zmq_recv (socket->pointer socket)
                                           (bytevector->pointer buffer)
                                           len 0)))
      (cond
       ((and (< result 0) (= errno 4)) (zmq-receive-bytevector socket len)) ;; apparently after getting EINTR error, socket should be read again
       ((< result 0) (zmq-get-error errno))
       ((= result 0) (make-bytevector 0))
       (else
	(let ((ret-length (min len result)))
	  (if (< ret-length len)
	      (let ((ret (make-bytevector ret-length))) ;; create a bytevector having a length of received message
	        (bytevector-copy! buffer 0 ret 0 ret-length)
		ret)
	      buffer)))))))

(define (zmq-receive socket len)
  (let  ((buffer (zmq-receive-bytevector socket len)))
    (bv->string buffer)))

(define* (zmq-send-bytevector socket data #:optional (flag 0))
  (let*  ((len (bytevector-length data)))
    (let-values (((result errno) (zmq_send (socket->pointer socket)
                                           (bytevector->pointer data)
                                           len flag)))
      (cond
       ((and (< result 0) (= errno 4))
        (zmq-send-bytevector socket data flag))
       ((< result 0)
        (zmq-get-error errno))))))

(define* (zmq-send socket data #:optional (flag 0))
  (let*  ((buffer (string->bv data)))
    (zmq-send-bytevector socket buffer flag)))

(define (zmq-message-receive socket message)
  (let-values (((result errno) (zmq_msg_recv message
                                             (socket->pointer socket) 0)))
    (if (= result -1)
	(zmq-get-error errno)
        message)))

(define (zmq-message-content message)
  (let ((content-ptr (zmq_msg_data message)))
    (if (null-pointer? content-ptr)
	(zmq-get-error-msg "Function zmq-message-content failed.")
	content-ptr)))

;;
;; Message parts.
;;

(define* (zmq-get-msg-parts-bytevector socket #:optional (parts '()))
  (let* ((part      (zmq-receive-bytevector socket BUF-SIZE))
         (new-parts (append parts (list part)))
         (opt       (zmq-get-socket-option socket ZMQ_RCVMORE)))
    (if (> opt 0)
        (zmq-get-msg-parts-bytevector socket new-parts)
        new-parts)))

(define (zmq-send-msg-parts-bytevector socket parts)
  (if (not (null? parts))
      (let* ((data (car parts))
	     (flag  (if (> (length parts) 1) ZMQ_SNDMORE 0)))
	(zmq-send-bytevector socket data flag)
	(zmq-send-msg-parts-bytevector socket (cdr parts)))))

(define* (zmq-get-msg-parts socket #:optional (parts '()))
  (bv-list->string-list
   (zmq-get-msg-parts-bytevector socket parts)))

(define (zmq-send-msg-parts socket parts)
  (let ((bv-parts (string-list->bv-list parts)))
    (zmq-send-msg-parts-bytevector socket bv-parts)))

;; convert ascii string into bytevector
(define (string->bv text)
  (string->bytevector text MSG-ENCODING))

;; convert bytevector into ascii string
(define (bv->string bv)
  (bytevector->string bv MSG-ENCODING))

;; convert a list of strings to a list of bytevectors
(define (string-list->bv-list str-list)
  (map string->bv str-list))

;; convert a list of bytevectors to a list of strings
(define (bv-list->string-list bv-list)
  (map bv->string bv-list))

(define %pollitem-struct
  ;; The 'zmq_pollitem_t' struct.
  (list '* int short short))

(define (poll-items->array lst)
  "Return a C array of 'zmq_pollitem_t' structures made from LST, a list of
<poll-item>, along with the number of elements in that array."
  (define pollitem-size
    (sizeof %pollitem-struct))

  (let* ((len   (length lst))
         (array (make-bytevector (* len pollitem-size))))
    (let loop ((index 0)
               (lst lst))
      (match lst
        (()
         (values (bytevector->pointer array) len))
        ((($ <poll-item> socket fd events) . tail)
         ;; XXX: A shortcoming of the FFI forces us to allocate an
         ;; intermediate C structure.  Oh well.
         (let ((struct (make-c-struct %pollitem-struct
                                      (list (if socket
                                                (socket->pointer socket)
                                                %null-pointer)
                                            fd
                                            events
                                            0))))
           (bytevector-copy! (pointer->bytevector struct pollitem-size) 0
                             array index pollitem-size)
           (loop (+ pollitem-size index) tail)))))))

(define (array->poll-items array len)
  "Turn ARRAY, a C array of LEN 'zmq_pollitem_t' elements, into a list of
<poll-item> records.  Structures with an 'revents' field equal to zero are
omitted."
  (define pollitem-size
    (sizeof %pollitem-struct))

  (define (pointer+ pointer offset)
    (make-pointer (+ (pointer-address pointer) offset)))

  (let loop ((index 0)
             (result '()))
    (if (= index (* len pollitem-size))
        (reverse result)
        (loop (+ index pollitem-size)
              (match (parse-c-struct (pointer+ array index)
                                     %pollitem-struct)
                ((socket fd events 0)             ;no events
                 result)
                ((socket fd events revents)
                 (cons (poll-item (if (null-pointer? socket)
                                      #f
                                      (pointer->socket socket))
                                  revents
                                  #:fd fd)
                       result)))))))

(define* (zmq-poll items #:optional (timeout -1))
  "Poll on the given ITEMS, a list of <poll-item> records; wait until one of
the requested events occurred or TIMEOUT has expired.  When TIMEOUT is -1,
wait indefinitely.  Return a possibly empty list of <poll-item> records
denoting the events that occurred."
  (let*-values (((array length)  (poll-items->array items))
                ((result errno) (zmq_poll array length timeout)))
    (if (>= result 0)
        (array->poll-items array length)
        (zmq-get-error errno))))

(define* (zmq-proxy frontend backend #:optional (capture #f))
  "Start built-in proxy connecting FRONTEND to BACKEND.  If CAPTURE socket is
set the proxy shall send all messages received on both FRONTEND and BACKEND to
the CAPTURE socket."
  (zmq_proxy (socket->pointer frontend)
             (socket->pointer backend)
             (if capture
                 (socket->pointer capture)
                 %null-pointer)))
