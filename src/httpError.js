function HttpError(message, statusCode) {
  const instance = new Error(message);
  instance.name = 'HttpError';
  instance.statusCode = statusCode;
  Object.setPrototypeOf(instance, Object.getPrototypeOf(this));
  return instance;
}

module.exports = HttpError;
