/**
 * This file defines frontend behavior for DropZone.
 * It is part of the IICPC frontend and keeps UI, API, or test behavior
 * scoped to this module so callers can rely on stable boundaries.
 */
"use client";

import { useRef, useState } from "react";
import { Archive, UploadCloud, X } from "lucide-react";
import { platformConfig } from "@/config/platform";
import styles from "./DropZone.module.css";

/**
 * Props describes structured data exchanged by this module.
 * Keep this shape aligned with API and component expectations.
 */

interface Props {
  file: File | null;
  error: string | null;
  onFile: (file: File | null) => void;
}

/**
 * DropZone performs the module-specific operation described by its name.
 * It keeps inputs, side effects, and returned values within this module's contract.
 */
export function DropZone({ file, error, onFile }: Props) {
  const inputRef = useRef<HTMLInputElement | null>(null);
  const [active, setActive] = useState(false);

  const acceptFile = (candidate: File | null) => {
    if (!candidate) return;
    onFile(candidate);
  };

  return (
    <div className={styles.wrap}>
      <button
        className={`${styles.zone} ${active ? styles.active : ""}`}
        type="button"
        onClick={() => inputRef.current?.click()}
        onDragOver={(event) => {
          event.preventDefault();
          setActive(true);
        }}
        onDragLeave={() => setActive(false)}
        onDrop={(event) => {
          event.preventDefault();
          setActive(false);
          acceptFile(event.dataTransfer.files.item(0));
        }}
      >
        {file ? (
          <>
            <span className={styles.icon} aria-hidden="true">
              <Archive size={22} strokeWidth={1.8} />
            </span>
            <span className={styles.file}>{file.name}</span>
            <span
              className={styles.clear}
              role="button"
              tabIndex={0}
              aria-label="Clear selected file"
              onClick={(event) => {
                event.stopPropagation();
                onFile(null);
              }}
            >
              <X size={16} strokeWidth={1.8} />
            </span>
          </>
        ) : (
          <>
            <span className={styles.icon} aria-hidden="true">
              <UploadCloud size={30} strokeWidth={1.6} />
            </span>
            <span>Drop your .zip bundle here</span>
            <small>or click to browse</small>
          </>
        )}
      </button>
      <input
        ref={inputRef}
        className={styles.input}
        type="file"
        accept=".zip,application/zip"
        onChange={(event) => acceptFile(event.target.files?.item(0) ?? null)}
      />
      {(error || (file && file.size > platformConfig.uploadMaxBytes)) && (
        <p className={styles.error}>
          {error ??
            `File exceeds ${Math.round(platformConfig.uploadMaxBytes / 1024 / 1024)} MB limit`}
        </p>
      )}
    </div>
  );
}
