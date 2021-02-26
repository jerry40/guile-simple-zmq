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
  #:use-module (ice-9 threads)
  #:use-module (srfi srfi-1)
  #:use-module (srfi srfi-64))

(test-begin "base")

(define %zmq-context
  (zmq-create-context))

(define zmq-endpoint
  "ipc:///tmp/messages")

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

(test-equal "msg-init-size"
  5
  (let ((msg (zmq-msg-init-size 5)))
    (zmq-message-size msg)))

(define test-iterations 2)

(define (test-sender)
  (define send-socket
    (zmq-create-socket %zmq-context ZMQ_PUSH))

  (zmq-bind-socket send-socket zmq-endpoint)

  (let loop ((i 1))
    (unless (> i test-iterations)
      (zmq-send-msg-parts-bytevector
       send-socket
       (list (string->bv (number->string i))
             (string->bv "test")))
      (loop (+ i 1))))

  (zmq-close-socket send-socket))

(define (test-receiver)
  (let* ((recv-socket (zmq-create-socket %zmq-context ZMQ_PULL))
         (connect (zmq-connect recv-socket zmq-endpoint))
         (poll-items (list (poll-item recv-socket ZMQ_POLLIN))))
    (gc)
    (let loop ((i 1)
               (messages '()))
      (if (> i test-iterations)
          (begin
            (zmq-close-socket recv-socket)
            messages)
          (let* ((items (zmq-poll* poll-items 1000))
                 (messages* (zmq-message-receive recv-socket)))
            (gc)
            (loop (1+ i)
                  (append messages
                          (map (compose bv->string zmq-message-content)
                               messages*))))))))

(test-equal "messages"
  '("1" "test" "2" "test")
  (begin
    (call-with-new-thread
     (lambda ()
       (test-sender)))
    (test-receiver)))

(zmq-destroy-context %zmq-context)

(test-end)
