"""Post-SQL engine – identical to PreSQLEngine but executed *after* the main
pipeline.  We simply subclass the existing PreSQLEngine so we inherit all
features (error-handling, analytics, progress integration)."""

from tlptaco.engines.presql import PreSQLEngine as _Base


class PostSQLEngine(_Base):
    """Post-SQL engine (reuses PreSQLEngine logic but different progress label)."""

    def __init__(self,
                 files_cfg,
                 runner,
                 logger=None,
                 user_list: list[str] | None = None):
        # Pass layer_name="Post-SQL" so progress manager recognises the label
        super().__init__(files_cfg, runner, logger, user_list, layer_name="Post-SQL")

    # Cosmetic repr for logging/debugging
    def __repr__(self):  # pragma: no cover – cosmetic
        return f"<PostSQLEngine tasks={self.num_steps()}>"
