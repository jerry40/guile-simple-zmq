;;; Guile-Simple-ZMQ --- ZeroMQ bindings for GNU Guile.
;;; Copyright © 2021 Mathieu Othacehe <othacehe@gnu.org>
;;;
;;; This file is part of Guile-Simple-ZMQ.
;;;
;;; Guile-Simple-ZMQ is free software; you can redistribute it and/or modify
;;; it under the terms of the GNU General Public License as published by the
;;; Free Software Foundation; either version 3 of the License, or (at your
;;; option) any later version.
;;;
;;; Guile-Simple-ZMQ is distributed in the hope that it will be useful, but
;;; WITHOUT ANY WARRANTY; without even the implied warranty of
;;; MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;;; GNU General Public License for more details.
;;;
;;; You should have received a copy of the GNU General Public License
;;; along with Guile-Simple-ZMQ.  If not, see <http://www.gnu.org/licenses/>.

(define-module (tests base)
  #:use-module (simple-zmq)
  #:use-module (ice-9 match)
  #:use-module (ice-9 threads)
  #:use-module (srfi srfi-1)
  #:use-module (srfi srfi-64))

(test-begin "base")

(define %zmq-context
  (zmq-create-context))

(define zmq-endpoint
  "ipc:///tmp/messages")

(define zmq-socks-proxy
  "")

(define zmq-plain-username
  "")

(define zmq-bind-to-device
  "")

(define zmq-rcv-snd-buf-size
  BUF-SIZE)

(define zmq-connect-timeout
  1000)

(define (EINTR-safe proc)
  "Return a variant of PROC that catches EINTR 'zmq-error' exceptions and
retries a call to PROC."
  (define (safe . args)
    (catch 'zmq-error
      (lambda ()
        (apply proc args))
      (lambda (key errno . rest)
        (if (= errno EINTR)
            (apply safe args)
            (apply throw key errno rest)))))

  safe)

(define zmq-poll*
  ;; Return a variant of ZMQ-POLL that catches EINTR errors.
  (EINTR-safe zmq-poll))

(test-assert "get-option"
  (positive?
   (zmq-get-context-option %zmq-context ZMQ_MSG_T_SIZE)))

(test-equal "msg-init"
  "alice"
  (let ((msg (zmq-msg-init (string->bv "alice"))))
    (bv->string (zmq-message-content msg))))

(define test-iterations 2)

(define (test-sender)
  (define send-socket
    (zmq-create-socket %zmq-context ZMQ_PUSH))

  (zmq-bind-socket send-socket zmq-endpoint)

  (let loop ((i 1))
    (unless (> i test-iterations)
      (zmq-message-send-parts
       send-socket
       (map zmq-msg-init
            (list (string->bv (number->string i))
                  (string->bv "test"))))
      (loop (+ i 1))))

  (zmq-close-socket send-socket))

(define (test-receiver socket)
  (let* ((connect (zmq-connect socket zmq-endpoint))
         (poll-items (list (poll-item socket ZMQ_POLLIN))))
    (gc)
    (let loop ((i 1)
               (messages '()))
      (if (> i test-iterations)
          messages
          (let* ((items (zmq-poll* poll-items 1000))
                 (messages* (zmq-message-receive socket)))
            (gc)
            (loop (1+ i)
                  (append messages
                          (map (compose bv->string zmq-message-content)
                               messages*))))))))

(define %recv-socket
  (zmq-create-socket %zmq-context ZMQ_PULL))

(test-equal "messages"
  '("1" "test" "2" "test")
  (begin
    (call-with-new-thread
     (lambda ()
       (test-sender)))
    (test-receiver %recv-socket)))

(test-equal "get-socket-option TYPE"
  ZMQ_PULL
  (zmq-get-socket-option %recv-socket ZMQ_TYPE))

(test-equal "get-socket-option ENDPOINT"
  zmq-endpoint
  (zmq-get-socket-option %recv-socket ZMQ_LAST_ENDPOINT))

(test-equal "get-socket-option SOCKS_PROXY"
  zmq-socks-proxy
  (zmq-get-socket-option %recv-socket ZMQ_SOCKS_PROXY))

(test-equal "get-socket-option PLAIN_USERNAME"
  zmq-plain-username
  (zmq-get-socket-option %recv-socket ZMQ_PLAIN_USERNAME))

(test-equal "get-socket-option BINDTODEVICE"
  zmq-bind-to-device
  (zmq-get-socket-option %recv-socket ZMQ_BINDTODEVICE))

(test-equal "get-socket-option EVENTS"
  (list 0 ZMQ_POLLIN)
  (let ((first (zmq-get-socket-option %recv-socket ZMQ_EVENTS))
        (second (let ((send-socket (zmq-create-socket %zmq-context ZMQ_PUSH)))
                  (zmq-bind-socket send-socket zmq-endpoint)
                  (zmq-send-bytevector send-socket #vu8(1 2 3))
                  (zmq-close-socket send-socket)
                  (match (zmq-get-socket-option %recv-socket ZMQ_EVENTS)
                    (0
                     ;; It might take a bit longer for the message to reach
                     ;; its destination, so wait for it.
                     (sleep 5)
                     (zmq-get-socket-option %recv-socket ZMQ_EVENTS))
                    (flags flags)))))
    (list first second)))

;; set underlying kernel send/receive buffer size for the socket
(zmq-set-socket-option %recv-socket ZMQ_SNDBUF zmq-rcv-snd-buf-size)
(zmq-set-socket-option %recv-socket ZMQ_RCVBUF zmq-rcv-snd-buf-size)

(test-equal "get-socket-option SNDBUF"
  zmq-rcv-snd-buf-size
  (zmq-get-socket-option %recv-socket ZMQ_SNDBUF))

(test-equal "get-socket-option RCVBUF"
  zmq-rcv-snd-buf-size
  (zmq-get-socket-option %recv-socket ZMQ_RCVBUF))

;; set connection timeout for the socket
(zmq-set-socket-option %recv-socket ZMQ_CONNECT_TIMEOUT zmq-connect-timeout)

(test-equal "get-socket-option CONNECT_TIMEOUT"
  zmq-connect-timeout
  (zmq-get-socket-option %recv-socket ZMQ_CONNECT_TIMEOUT))

(zmq-close-socket %recv-socket)
(zmq-destroy-context %zmq-context)

(test-end)
