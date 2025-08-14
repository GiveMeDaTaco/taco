"""Post-SQL engine – identical to PreSQLEngine but executed *after* the main
pipeline.  We simply subclass the existing PreSQLEngine so we inherit all
features (error-handling, analytics, progress integration)."""

from tlptaco.engines.presql import PreSQLEngine as _Base


class PostSQLEngine(_Base):
    """Alias class with no behaviour changes – used for semantic clarity."""

    # The implementation is entirely inherited from PreSQLEngine.  We only
    # override __repr__ so logging messages are clearer.

    def __repr__(self):  # pragma: no cover – cosmetic
        return f"<PostSQLEngine tasks={self.num_steps()}>"
